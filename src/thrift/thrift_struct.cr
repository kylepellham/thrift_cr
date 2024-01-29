require "./types.cr"
require "./helpers.cr"

module Thrift
  module Struct
    annotation Property
    end

    private macro generate_writer
      def write(oprot : ::Thrift::BaseProtocol)
        validate
        oprot.write_struct_begin(\{{@type.stringify}})
        \{% for var in @type.instance_vars %}
          oprot.write_field_begin(\{{var.name.stringify}}, @\{{var}}.thrift_type, \{{var.annotation(Property)[:id]}}.to_i16)
          @\{{var}}.write(oprot)         
        \{% end %}
        oprot.write_field_stop
      end
      def self.instance_vars

        return \{{@type.instance_vars.map &.stringify}}
      end
    end

    private macro generate_reader
      def self.read(iprot)
        \{% begin %}
        \{% for var in @type.instance_vars %}
          \{{var}}_instance = nil
        \{% end %}
        \{% end %}
        iprot.read_struct_begin
        loop do
          name, type, fid = iprot.read_field_begin
          break if type == ::Thrift::Types::Stop
          \{% begin %}
          case fid
          \{% for var in @type.instance_vars %}
            when \{{var.annotation(Property)[:id]}}
              \{{var}}_instance = \{{var.type}}.read(iprot)
          \{% end %}
          else
            raise "Not a Possible field #{fid}"
          end
          \{% end %}
          iprot.read_field_end
        end
        iprot.read_struct_end
        \{% begin %}
        return \{{@type}}.new(
          \{% for var in @type.instance_vars %}
            \{{var}}: \{{var}}_instance.not_nil!,
          \{% end %}
        )
        \{% end %}
      end
    end

    macro included
      {% verbatim do %}
        define_thrift_type ::Thrift::Types::Struct
        generate_writer
        generate_reader
      {% end %}
    end
  end
end
        #   \{% if var.type.name(generic_args: false) == Bool %}
        #     \{% write_type = "::Thrift::Types::BOOL"
        #       send_func = "oprot.write_bool(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == String %}
        #     \{% write_type = "::Thrift::Types::STRING"
        #       send_func = "oprot.write_string(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Int8 %}
        #     \{% write_type = "::Thrift::Types::I8"
        #       send_func = "oprot.write_i8(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Int16 %}
        #     \{% write_type = "::Thrift::Types::I16"
        #       send_func = "oprot.write_i16(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false).id == "Int32".id %} 
        #     \{% write_type = "::Thrift::Types::I32"
        #       send_func = "oprot.write_i32(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Int64 %}
        #     \{% write_type = "::Thrift::Types::I64"
        #       send_func = "oprot.write_i64(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Float32 %}
        #     \{% write_type = "::Thrift::Types::FLOAT"
        #       send_func = "oprot.write_float(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Float64 %}
        #     \{% write_type = "::Thrift::Types::DOUBLE"
        #       send_func = "oprot.write_double(@\{{var}})" %}
        #   \{% elsif var.type.name(generic_args: false) == Array %}
        #     \{% write_type = "::Thrift::Types::LIST"
        #       send_func = "oprot.write_list(@\{{var}})" %}
        #   \{% end %}
