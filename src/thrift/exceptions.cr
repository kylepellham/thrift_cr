module Thrift
  class Exception < ::Exception
  end

  class ApplicationException < ::Thrift::Exception
    include ::Thrift::Type
    include ::Thrift::Type::Read
    extend ::Thrift::Type::ClassRead

    UNKNOWN                 =  0
    UNKNOWN_METHOD          =  1
    INVALID_MESSAGE_TYPE    =  2
    WRONG_METHOD_NAME       =  3
    BAD_SEQUENCE_ID         =  4
    MISSING_RESULT          =  5
    INTERNAL_ERROR          =  6
    PROTOCOL_ERROR          =  7
    INVALID_TRANSFORM       =  8
    INVALID_PROTOCOL        =  9
    UNSUPPORTED_CLIENT_TYPE = 10

    getter :type

    def initialize(type = UNKNOWN, message = nil)
      super(message)
      @type = type
    end

    def message
      "message: #{@message}, type: #{@type}"
    end

    def read(from iprot : ::Thrift::BaseProtocol)
      iprot.read_struct_begin
      loop do
        fname, ftype, fid = iprot.read_field_begin
        break if ftype == Types::Stop
        case {fid, ftype}
        when {1, String.thrift_type}
          @message = String.read from: iprot
        when {2, Int32.thrift_type}
          @type = Int32.read from: iprot
        else
          iprot.skip(ftype)
        end
        iprot.read_field_end
      end
      iprot.read_struct_end
    end

    def write(to oprot : ::Thrift::BaseProtocol)
      oprot.write_struct_begin("Thrift::ApplicationException")
      @message.try do |message|
        oprot.write_field_begin("message", message.thrift_type, 1_i16)
        message.write to: oprot
        oprot.write_field_end
      end

      @type.try do |type|
        oprot.write_field_begin("type", type.thrift_type, 2_i16)
        type.write to: oprot
        oprot.write_field_end
      end

      oprot.write_field_stop
      oprot.write_struct_end
    end
  end

  # Mixin that adds crystal safe #message for raising thrift exceptions
  module ExceptionAdapter

    # xception_getter should be used in thrift exceptions because exceptions should be immutable
    macro xception_getter(name)
      def {{name.var.id}} : {{name.type}}
        @{{name.var.id}}
      end

      @{{name}}
    end

    def message
      {% begin %}
      first = true
      %message = ""
      {% for var in @type.methods.select(&.annotation(::Thrift::Struct::Property)) %}
        if !@{{var.name.id}}.nil?
          if first
            first = false
          else
            %message += ", "
          end
          %message += "{{var.name.id}}: #{@{{var.name.id}}}"
        end
      {% end %}
      {% end %}
    end
  end
end
