require "./types.cr"
require "./helpers.cr"

module Thrift
  module Struct
    annotation Property
    end

    macro struct_property(name)
      {% if name.is_a?(TypeDeclaration) %}
        {% if name.value %}
          @{{name.var.id}} : {{name.type.id}}? = {{name.value}}
        {% else %}
          @{{name.var.id}} : {{name.type.id}}?
        {% end %}

        def {{name.var.id}}
          \{% if var = @type.instance_vars.find{|var| var.name.stringify == {{name.var.stringify}} }  &&
                  var.annotation(Thrift::Struct::Property)[:req_in]%}
            @{{name.var.id}}.not_nil!
          \{% else %}
            @{{name.var.id}}
          \{% end %}
        end

        def {{name.var.id}}=({{name.var.id}}_ : {{name.type.id}})
          @{{name.var.id}} = {{name.var.id}}_
        end
      {% else %}
        {{raise "thrift_property is for non union type structs only i.e thrift_struct x : Int32"}}
      {% end %}
    end

    private macro generate_writer
      def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
        validate
        oprot.write_struct_begin(\{{@type.stringify}})
        \{% for var in @type.instance_vars %}
          oprot.write_field_begin(\{{var.name.stringify}}, @\{{var.name.id}}.thrift_type, \{{var.annotation(Property)[:id]}}.to_i16)
          \{% if var.annotation(Thrift::Struct::Property)[:req_out] %}
            raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN,
                      "Required field \{{var.name.id}} is unset!") if @\{{var.name}}.nil?
          \{% end %}
          @\{{var.name.id}}.write(oprot)
          oprot.write_field_end
        \{% end %}
        oprot.write_field_stop
        oprot.write_struct_end
      end

      def self.instance_vars
        return \{{@type.instance_vars.map &.stringify}}
      end
    end

    private macro generate_reader
      def self.read(iprot)
        recieved_struct = \{{@type.id}}.new
        iprot.read_struct_begin
        loop do
          name, type, fid = iprot.read_field_begin
          # puts fid
          break if type == ::Thrift::Types::Stop
          next if type == ::Thrift::Types::Void
          \{% begin %}
          case fid
          \{% for var in @type.instance_vars %}
            \{% if var.type.union_types.size < 3 %}
              \{% type = var.type.union_types.find { |type| type.stringify != "Nil" } %}
            \{% else %}
              \{{raise "Union too large for thrift struct. Nilable types only"}}
            \{% end %}
            # \{{puts var.annotation(::Thrift::Struct::Property)[:id].id}}
            when \{{var.annotation(::Thrift::Struct::Property)[:id].id}}
              recieved_struct.\{{var}} = \{{type}}.read(iprot)
          \{% end %}
          else
            raise "Not a Possible field #{fid}"
          end
          \{% end %}
          iprot.read_field_end
        end
        iprot.read_struct_end
        recieved_struct.validate
        return recieved_struct
      end
    end

    macro included
      {% verbatim do %}
        define_thrift_type ::Thrift::Types::Struct
        generate_writer
        generate_reader
      {% end %}

      def validate
        {% for var in @type.instance_vars %}
          {% if var.annotation(Thrift::Struct::Property)[:in_req] %}
            raise Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN,
                                                "Required field {{var.name}} is unset") if {{var.name.id}}.nil?
          {% end %}
        {% end %}
      end
    end
  end
end