require "log"
require "./types.cr"
require "./helpers.cr"

module Thrift

  # mixing module that will define a class level read, protected instance level read and an instance level write method
  # macro struct_propety is included that MUST be used when defining properties on a thrift generated class
  module Struct
    macro struct_property(name)
      def {{name.var.id}} : {{name.type.id}}
        @{{name.var.id}}
      end

      @{{name}}

      def {{name.var.id}}=({{name.var.id}})
        \{% if (annotated_getter = @type.methods.find{|method| method.name.symbolize == {{name.var.symbolize}} }) && annotated_getter.annotation(::Thrift::Type::Properties) && annotated_getter.annotation(::Thrift::Type::Properties)[:requirement] == :optional %}
          @__isset.{{name.var.id}} = !{{name.var.id}}.nil?
        \{% end %}
        @{{name.var.id}} = {{name.var.id}}
      end
    end

    def <=>(other : self)
      {% for var in @type.instance_vars %}
      if @{{var.name.id}}.nil? && !other.@{{var.name.id}}.nil?
        return 1
      elsif !@{{var.name.id}}.nil? && other.@{{var.name.id}}.nil?
        return -1
      else
        if ({{var.name.id}} = @{{var.name.id}}) && ({{var.name.id}}_other = other.@{{var.name.id}})
          if ({{var.name.id}}.responds_to?(:<=>))
            cmp = {{var.name.id}} <=> {{var.name.id}}_other
            return cmp if cmp != 0
          end
        end
      end
      {% end %}
      0
    end



    def write(to oprot : ::Thrift::BaseProtocol)
      {% begin %}
      {%
          requires_write = @type.methods.select{|method| method.annotation(::Thrift::Type::Properties) && method.annotation(::Thrift::Type::Properties)[:requirement] == :required}
          opt_in_req_out_write = @type.methods.select{|method| method.annotation(::Thrift::Type::Properties) && method.annotation(::Thrift::Type::Properties)[:requirement] == :opt_in_req_out}
          optional_write = @type.methods.select{|method| method.annotation(::Thrift::Type::Properties) && method.annotation(::Thrift::Type::Properties)[:requirement] == :optional}
      %}

      {% if !opt_in_req_out_write.empty? %}
        unless (%empty_fields = { {{opt_in_req_out_write.map{|write| "#{write.name.stringify} => !#{write.name.id}.nil?".id}.splat }} }.select{|k,v| !v}).empty?
          ::Log.for(self.class).error {"Required Field(s) missing during write: #{%empty_fields.keys.join(", ")}"}
        end
      {% end %}
      oprot.write_recursion do
        oprot.write_struct_begin(self.class.name)

        {% for write in (requires_write + opt_in_req_out_write) %}
          oprot.write_field_begin({{write.annotation(::Thrift::Type::Properties)[:transmit_name] || write.name.stringify}}, @{{write.name.id}}.thrift_type, {{write.annotation(::Thrift::Type::Properties)[:fid].id}}_i16)
          @{{write.name.id}}.write to: oprot
          oprot.write_field_end
        {% end %}

        {% for write in optional_write %}
          @__isset.{{write.name.id}} && @{{write.name.id}}.try do |{{write.name.id}}|
            oprot.write_field_begin({{write.annotation(::Thrift::Type::Properties)[:transmit_name] || write.name.stringify}}, {{write.name.id}}.thrift_type, {{write.annotation(::Thrift::Type::Properties)[:fid].id}}_i16)
            {{write.name.id}}.write to: oprot
            oprot.write_field_end
          end
        {% end %}
        oprot.write_field_stop
        oprot.write_struct_end
      end
      {% end %}
    end

    protected def read(from iprot : ::Thrift::BaseProtocol)
      {% begin %}

      {% requires_check = @type.methods.select{|method| method.annotation(::Thrift::Type::Properties) && method.annotation(::Thrift::Type::Properties)[:requirement] == :required} %}
      iprot.read_recursion do
        {% if !requires_check.empty? %}
          %required_fields_set = {
            {{
              requires_check.map do |required|
                "#{required.stringify} => false".id
              end.splat
            }}
          }
        {% end %}
        iprot.read_struct_begin
        loop do
          name, ftype, fid = iprot.read_field_begin
          break if ftype == ::Thrift::Types::Stop
          next if ftype == ::Thrift::Types::Void
          case {fid, ftype}
          {% for var in @type.methods.select(&.annotation(::Thrift::Type::Properties)) %}
            {%
              type = if var.return_type == Nil
                raise "struct_property #{var.name} is a Nil"
              elsif var.return_type.is_a?(Union)
                var.return_type.types.find(&.!=(Nil))
              else
                var.return_type
              end
            %}
            when { {{var.annotation(::Thrift::Type::Properties)[:fid].id}}, {{type}}.thrift_type }
              @{{var.name.id}} = {{type}}.read from: iprot
              {% if var.annotation(::Thrift::Type::Properties)[:requirement] == :required %}
                %required_fields_set[{{var.name.stringify}}] = true
              {% elsif var.annotation(::Thrift::Type::Properties)[:requirement] == :optional %}
                @__isset.{{var.name.id}} = true
              {% end %}
          {% end %}
          else
            iprot.skip ftype
          end
          iprot.read_field_end
        end
        iprot.read_struct_end
        {% if !requires_check.empty? %}
          %required_fields_set.select!{|k,v| !v}
          raise ::Thrift::ProtocolException.new ::Thrift::ProtocolException::INVALID_DATA, "Required field(s) not set when reading: #{$required_fields_set.keys.join(", ")}" unless %required_fields_set.empty?
        {% end %}
      end
      {% end %}
    end

    macro included
      {% verbatim do %}
        include Comparable(self)
        include ::Thrift::Type
        include ::Thrift::Type::Read
        extend ::Thrift::Type::ClassRead
        define_thrift_type ::Thrift::Types::Struct
        macro finished
          {% begin %}
          {% has_optionals = false %}
          {% for optional in @type.methods.select{|method| method.annotation(::Thrift::Type::Properties) && method.annotation(::Thrift::Type::Properties)[:requirement] == :optional} %}
            {% has_optionals = true %}
            struct {{@type.id}}__isset
              property {{optional.name.id}} : Bool = false
            end
          {% end %}
          {% if has_optionals %}
            @__isset = {{@type.id}}__isset.new
          {% end %}
          {% end %}
        end
      {% end %}
    end
  end
end