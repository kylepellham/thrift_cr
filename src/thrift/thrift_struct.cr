require "./types.cr"
require "./helpers.cr"

module Thrift
  module Struct
    annotation Property
    end

    macro def_comp
      def <=>(other : self)
        {% for var in @type.instance_vars %}
        if @{{var.name.id}}.nil? && !other.@{{var.name.id}}.nil?
          return 1
        elsif !@{{var.name.id}}.nil? && other.@{{var.name.id}}.nil?
          return -1
        else
          cmp = @{{var.name.id}} <=> other.@{{var.name.id}}
          return cmp if cmp != 0
        end
        {% end %}
        0
      end
    end

    macro struct_property(name)
      def {{name.var.id}}
        @{{name.var.id}}
      end

      @{{name}}

      def {{name.var.id}}=({{name.var.id}})
        \{% if (annotated_getter = @type.methods.find{|method| method.name.symbolize == {{name.var.symbolize}} }) && annotated_getter.annotation(::Thrift::Struct::Property) && annotated_getter.annotation(::Thrift::Struct::Property)[:requirement] == :optional %}
          @__isset.{{name.var.id}} = !{{name.var.id}}.nil?
        \{% end %}
        @{{name.var.id}} = {{name.var.id}}
      end
    end

    private macro generate_struct_writer
      def write(to oprot : ::Thrift::BaseProtocol)
        \{% begin %}
        \{%
            requires_write = @type.methods.select{|method| method.annotation(::Thrift::Struct::Property) && method.annotation(::Thrift::Struct::Property)[:requirement] == :required}
            opt_in_req_out_write = @type.methods.select{|method| method.annotation(::Thrift::Struct::Property) && method.annotation(::Thrift::Struct::Property)[:requirement] == :opt_in_req_out}
            optional_write = @type.methods.select{|method| method.annotation(::Thrift::Struct::Property) && method.annotation(::Thrift::Struct::Property)[:requirement] == :optional}
        %}

        \{% if !opt_in_req_out_write.empty? %}
          unless (%empty_fields = { \{{opt_in_req_out_write.map{|write| "#{write.name.stringify} => !#{write.name.id}.nil?"}.splat }} }.select{|k,v| !v}).empty?
            raise ::Thrift::ProtocolException.new ::Thrift::ProtocolException::INVALID_DATA, "Required fields missing during write: #{%empty_fields.keys.join(", ")}"
          end
        \{% end %}
        oprot.write_recursion do
          oprot.write_struct_begin(self.class.name)

          \{% for write in (requires_write + opt_in_req_out_write) %}
            oprot.write_field_begin(\{{write.annotation(::Thrift::Struct::Property)[:transmit_name] || write.name.stringify}}, @\{{write.name.id}}.thrift_type, \{{write.annotation(::Thrift::Struct::Property)[:fid].id}}_i16)
            @\{{write.name.id}}.write to: oprot
            oprot.write_field_end
          \{% end %}

          \{% for write in optional_write %}
            @__isset.\{{write.name.id}} && @\{{write.name.id}}.try do |\{{write.name.id}}|
              oprot.write_field_begin(\{{write.annotation(::Thrift::Struct::Property)[:transmit_name] || write.name.stringify}}, \{{write.name.id}}.thrift_type, \{{write.annotation(::Thrift::Struct::Property)[:fid].id}}_i16)
              \{{write.name.id}}.write to: oprot
              oprot.write_field_end
          end
          \{% end %}
        end
        \{{debug}}
        \{% end %}
      end
    end

    private macro generate_struct_reader
      protected def read(from iprot : ::Thrift::BaseProtocol)
        \{% begin %}

        \{% requires_check = @type.methods.select{|method| method.annotation(::Thrift::Struct::Property) && method.annotation(::Thrift::Struct::Property)[:requirement] == :required} %}
        iprot.read_recursion do
          \{% if !requires_check.empty? %}
            %required_fields_set = {
              \{{
                requires_check.map do |required|
                  "#{required.stringify} => false".id
                end.splat
              }}
            }
          \{% end %}
          iprot.read_struct_begin
          loop do
            name, ftype, fid = iprot.read_field_begin
            break if ftype == ::Thrift::Types::Stop
            next if ftype == ::Thrift::Types::Void
            case {fid, ftype}
            \{% for var in @type.methods.select(&.annotation(::Thrift::Struct::Property)) %}
              \{%
                type = if var.return_type == Nil
                  raise "struct_property #{var.name} is a Nil"
                elsif var.return_type.is_a?(Union)
                  var.return_type.type.find(&.!=(Nil))
                else
                  var.return_type
                end
              %}
              when { \{{var.annotation(::Thrift::Struct::Property)[:id].id}}, \{{type.id}}.thrift_type }
                @\{{var}} = \{{type}}.read from: iprot
                \{% if var.annotation(::Thrift::Struct::Property)[:requirement] == :required %}
                  %required_fields_set[\{{var.name.stringify}}] = true
                \{% elsif var.annotation(::Thrift::Struct::Property)[:requirement] == :optional %}
                  @__isset.\{{var.name.id}} = true
                \{% end %}
            \{% end %}
            else
              iprot.skip ftype
            end
            iprot.read_field_end
          end
          iprot.read_struct_end
          \{% if !requires_check.empty? %}
            %required_fields_set.select!{|k,v| !v}
            raise ::Thrift::ProtocolException.new ::Thrift::ProtocolException::INVALID_DATA, "Required field(s) not set when reading: #{$required_fields_set.keys.join(", ")}" unless %required_fields_set.empty?
          \{% end %}
        end
        \{% end %}
      end
    end

    macro included
      {% verbatim do %}
        include Comparable(self)
        include ::Thrift::Type
        include ::Thrift::Type::Read
        extend ::Thrift::Type::ClassRead
        define_thrift_type ::Thrift::Types::Struct
        macro finished
          \{% begin %}
          generate_reader
          generate_writer
          def_equals_and_hash
          def_comp
          \{% has_optionals = false %}
          \{% for optional in @type.methods.select{|method| method.annotation(::Thrift::Struct::Property) && method.annotation(::Thrift::Struct::Property)[:requirement] == :optional} %}
            \{% has_optionals = true %}
            struct {{@type.id}}__isset
              property \{{optional.name.id}} : Bool = false
            end
          \{% end %}

          \{% if has_optionals %}
            @__isset = {{@type.id}}__isset.new
          \{% end %}

          \{% end %}
        end
      {% end %}
    end
  end
end