require "../types.cr"
require "../transport/base_transport.cr"

module Thrift

  class ProtocolException < Exception

    UNKNOWN = 0
    INVALID_DATA = 1
    NEGATIVE_SIZE = 2
    SIZE_LIMIT = 3
    BAD_VERSION = 4
    NOT_IMPLEMENTED = 5
    DEPTH_LIMIT = 6

    getter :type

    def initialize(@type=UNKNOWN, message=nil)
      super(message)
    end
  end

  class BaseProtocol
    getter trans : BaseTransport

    def initialize(@trans)
    end

    def native?
      puts "wrong method is being called!"
      false
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      raise NotImplementedError.new ""
    end

    def write_message_end; nil; end

    def write_struct_begin(name)
      raise NotImplementedError.new ""
    end

    def write_struct_end; nil; end

    def write_field_begin(name : String, type : MessageTypes, id : Int16)
      raise NotImplementedError.new ""
    end

    def write_field_end; nil; end

    def write_field_stop
      raise NotImplementedError.new ""
    end

    def write_map_begin(ktype, vtype, size)
      raise NotImplementedError.new ""
    end

    def write_map_end; nil; end

    def write_list_begin(etype, size)
      raise NotImplementedError.new ""
    end

    def write_list_end; nil; end

    def write_set_begin(etype, size)
      raise NotImplementedError.new ""
    end

    def write_set_end; nil; end

    def write_bool(bool : Boolean)
      raise NotImplementedError.new ""
    end

    def write_byte(byte : UInt8)
      raise NotImplementedError.new ""
    end

    def write_i16(i16 : Int16)
      raise NotImplementedError.new ""
    end

    def write_i32(i32 : Int32)
      raise NotImplementedError.new ""
    end

    def write_i64(i64 : Int64)
      raise NotImplementedError.new ""
    end

    def write_double(dub : Float64)
      raise NotImplementedError.new ""
    end

    # Writes a Thrift String. In Ruby 1.9+, the String passed will be transcoded to UTF-8.
    #
    # str - The String to write.
    #
    # Raises EncodingError if the transcoding to UTF-8 fails.
    #
    # Returns nothing.
    def write_string(str : String)
      raise NotImplementedError.new ""
    end

    # Writes a Thrift Binary (Thrift String with no encoding). In Ruby 1.9+, the String passed
    # will forced into BINARY encoding.
    #
    # buf - The String to write.
    #
    # Returns nothing.
    def write_binary(buf : Bytes)
      raise NotImplementedError.new ""
    end

    def read_message_begin : Tuple(String, UInt8, Int32)
      raise NotImplementedError.new ""
    end

    def read_message_end; nil; end

    def read_struct_begin
      raise NotImplementedError.new ""
    end

    def read_struct_end; nil; end

    def read_field_begin
      raise NotImplementedError.new ""
    end

    def read_field_end; nil; end

    def read_map_begin : Tuple(UInt8, UInt8, Int32)
      raise NotImplementedError.new ""
    end

    def read_map_end; nil; end

    def read_list_begin
      raise NotImplementedError.new ""
    end

    def read_list_end; nil; end

    def read_set_begin : Tuple(Uint8, Int32)
      raise NotImplementedError.new ""
    end

    def read_set_end; nil; end

    def read_bool
      raise NotImplementedError.new ""
    end

    def read_byte : Bool
      raise NotImplementedError.new ""
    end

    def read_i16 : Int16
      raise NotImplementedError.new ""
    end

    def read_i32 : Int32
      raise NotImplementedError.new ""
    end

    def read_i64 : Int64
      raise NotImplementedError.new ""
    end

    def read_double : Float64
      raise NotImplementedError.new ""
    end

    # Reads a Thrift String. In Ruby 1.9+, all Strings will be returned with an Encoding of UTF-8.
    #
    # Returns a String.
    def read_string : String
      raise NotImplementedError.new ""
    end

    # Reads a Thrift Binary (Thrift String without encoding). In Ruby 1.9+, all Strings will be returned
    # with an Encoding of BINARY.
    #
    # Returns a String.
    def read_binary : Bytes
      raise NotImplementedError.new ""
    end

    def write_field(*args)
      if args.size == 3
        field_info, fid, value = args
      elsif args.size == 4
        field_info = {:name => args[0], :type => args[1]}
        fid, value = args[2..3]
      else
        raise ArgumentError, "wrong number of arguments (#{args.size} for 3)"
      end

      write_field_begin(field_info[:name], field_info[:type], fid)
      write_type(field_info, value)
      write_field_end
    end


    def skip(type)
      case type
      when Types::BOOL
        read_bool
      when Types::BYTE
        read_byte
      when Types::I16
        read_i16
      when Types::I32
        read_i32
      when Types::I64
        read_i64
      when Types::DOUBLE
        read_double
      when Types::STRING
        read_string
      when Types::STRUCT
        read_struct_begin
        while true
          name, type, id = read_field_begin
          break if type == Types::STOP
          skip(type)
          read_field_end
        end
        read_struct_end
      when Types::MAP
        ktype, vtype, size = read_map_begin
        size.times do
          skip(ktype)
          skip(vtype)
        end
        read_map_end
      when Types::SET
        etype, size = read_set_begin
        size.times do
          skip(etype)
        end
        read_set_end
      when Types::LIST
        etype, size = read_list_begin
        size.times do
          skip(etype)
        end
        read_list_end
      else
        raise ProtocolException.new(ProtocolException::INVALID_DATA, "Invalid data")
      end
    end

    def to_s
      "#{trans.to_s}"
    end
  end

  class BaseProtocolFactory
    def get_protocol(trans)
      raise NotImplementedError.new ""
    end

    def to_s
      "base"
    end
  end
end


