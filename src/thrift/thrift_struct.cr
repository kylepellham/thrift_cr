require "./types.cr"
require "./helpers.cr"

module Thrift
  module Struct
    annotation Property
    end

    macro tstruct_property(name)
      {% if name.is_a?(TypeDeclaration) %}
        property {{name.var.id}} : {{name.type.id}}?
      {% else %}
        {{raise "Must be a type declaration"}}
      {% end %}
    end

    private macro generate_writer
      def write(oprot : ::Thrift::BaseProtocol)
        validate
        oprot.write_struct_begin(\{{@type.stringify}})
        \{% for var in @type.instance_vars %}
          oprot.write_field_begin(\{{var.name.stringify}}, @\{{var}}.thrift_type, \{{var.annotation(Property)[:id]}}.to_i16)
          @\{{var}}.write(oprot)
          oprot.write_field_end()
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
        recieved_struct = \{{@type.id}}.new()
        iprot.read_struct_begin
        loop do
          name, type, fid = iprot.read_field_begin
          break if type == ::Thrift::Types::Stop
          next if type == ::Thrift::Types::Void
          \{% begin %}
          case fid
          \{% for var in @type.instance_vars %}
            \{% if var.type.union_types.size < 3 %}
              \{% type = var.type.union_types.select { |type| type.stringify != "Nil" }[0] %}
            \{% else %}
              \{{raise "Union too large for thrift struct. Nilable types only"}}
            \{% end %}
            when \{{var.annotation(Property)[:id]}}
              \{{@type}}.\{{var}} = \{{type}}.read(iprot)
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
