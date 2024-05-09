require "./types.cr"
require "./helpers.cr"

module Thrift
  module Struct
    annotation Property
    end

    macro struct_property(name)
      {% if !name.type.is_a?(Union) && name.value %}
        {{raise "Required fields do not have default values"}}
      {% elsif name.type.is_a?(Union) && name.type.types.size > 2 && !name.type.types.any?(&.stringify.==("::Nil"))%}
        {{raise "only can pass in a single nilable type"}}
      {% end %}
      @{{name}}

      def {{name.var.id}}
        @{{name.var.id}}
      end

      def {{name.var.id}}=(@{{name.var.id}} : {{name.type.id}})
        # this means that the field is required
        {% if !name.type.is_a?(Union) %}
          @required_fields[{{name.var.stringify}}] = true
        {% end %}
      end
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
              \{{raise "Union too large for thrift struct. Sing Nilable types only"}}
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
      {% end %}
    end
  end
end