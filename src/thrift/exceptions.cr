module Thrift
  class ApplicationException < Exception

    UNKNOWN = 0
    UNKNOWN_METHOD = 1
    INVALID_MESSAGE_TYPE = 2
    WRONG_METHOD_NAME = 3
    BAD_SEQUENCE_ID = 4
    MISSING_RESULT = 5
    INTERNAL_ERROR = 6
    PROTOCOL_ERROR = 7
    INVALID_TRANSFORM = 8
    INVALID_PROTOCOL = 9
    UNSUPPORTED_CLIENT_TYPE = 10

    getter :type

    def initialize(type=UNKNOWN, message=nil)
      super(message)
      @type = type
    end

    def read(iprot)
      iprot.read_struct_begin
      while true
        fname, ftype, fid = iprot.read_field_begin
        if ftype == Types::STOP
          break
        end
        if fid == 1 && ftype == Types::STRING
          @message = iprot.read_string
        elsif fid == 2 && ftype == Types::I32
          @type = iprot.read_i32
        else
          iprot.skip(ftype)
        end
        iprot.read_field_end
      end
      iprot.read_struct_end
    end

    def write(oprot)
      oprot.write_struct_begin("Thrift::ApplicationException")
      if message = @message
        oprot.write_field_begin("message", Types::STRING, 1)
        oprot.write_string(message)
        oprot.write_field_end
      end
      if type = @type
        oprot.write_field_begin("type", Types::I32, 2)
        oprot.write_i32(type)
        oprot.write_field_end
      end
      oprot.write_field_stop
      oprot.write_struct_end
    end

  end
end
