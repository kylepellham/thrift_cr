module Thrift
  class ApplicationException < Exception
    include ::Thrift::Struct

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

    def read(from iprot : ::Thrift::BaseProtocol)
      iprot.read_struct_begin
      while true
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
      ret
    end

    def write(to oprot : ::Thrift::BaseProtocol)
      oprot.write_struct_begin("Thrift::ApplicationException")
      @message.try do |message|
        oprot.write_field_begin("message", message.thrift_type, 1_i16)
        message.write to: oprot
        oprot.write_field_end
      end

      @type.try do |type|
        oprot.write_field_begin("type", message.thrift_type, 2_i16)
        type.write to: oprot
        oprot.write_field_end
      end

      oprot.write_field_stop
      oprot.write_struct_end
    end
  end
end
