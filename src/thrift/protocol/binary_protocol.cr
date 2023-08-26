require "./base_protocol.cr"

module Thrift
  class BinaryProtocol < BaseProtocol
    VERSION_MASK = 0xffff0000_u32
    VERSION_1 = 0x80010000_u32
    TYPE_MASK = 0x000000ff_u32

    getter :strict_read, :strict_write

    def initialize(trans, @strict_read=true, @strict_write=true)
      super(trans)
      # Pre-allocated read buffer for fixed-size read methods. Needs to be at least 8 bytes long for
      # read_i64() and read_double().
      @rbuf = Bytes.new(8, 0)
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      # this is necessary because we added (needed) bounds checking to 
      # write_i32, and 0x80010000 is too big for that.
      if strict_write
        p! (0xffff_u16 & (VERSION_1 >> 16)).unsafe_as(Int16)
        write_i16((0xffff_u16 & (VERSION_1 >> 16)).unsafe_as(Int16))
        write_i16(type.to_i16)
        write_string(name)
        write_i32(seqid)
      else
        write_string(name)
        write_byte(type.to_u8)
        write_i32(seqid)
      end
    end

    def write_struct_begin(name); nil; end

    def write_field_begin(name : String, type : MessageTypes, id : Int16)
      write_byte(type.to_u8)
      write_i16(id)
    end

    def write_field_stop
      write_byte(Thrift::Types::STOP.to_u8)
    end

    def write_map_begin(ktype, vtype, size)
      write_byte(ktype)
      write_byte(vtype)
      write_i32(size)
    end

    def write_list_begin(etype, size)
      write_byte(etype)
      write_i32(size)
    end

    def write_set_begin(etype, size)
      write_byte(etype)
      write_i32(size)
    end

    def write_bool(bool)
      write_byte(bool ? 1 : 0)
    end

    def write_byte(byte : UInt8)
      raw = Bytes.new(1, 0)
      IO::ByteFormat::BigEndian.encode(byte, raw)
      trans.write(raw)
    end

    def write_i16(i16 : Int16)
      puts i16
      raw = Bytes.new(2, 0)
      IO::ByteFormat::BigEndian.encode(i16, raw)
      p! raw
      trans.write(raw)
    end

    def write_i32(i32 : Int32)
      puts i32
      raw = Bytes.new(4, 0)
      IO::ByteFormat::BigEndian.encode(i32, raw)
      trans.write(raw)
    end

    def write_i64(i64 : Int64)
      raw = Bytes.new(8, 0)
      IO::ByteFormat::BigEndian.encode(i64, raw)
      trans.write(raw)
    end

    def write_double(dub : Float64)
      raw = Bytes.new(8, 0)
      IO::ByteFormat::BigEndian.encode(dub, raw)
      trans.write(raw)     
    end

    def write_string(str : String)
      buf = str.encode("utf-8")
      write_binary(buf)
    end

    def write_binary(buf)
      write_i32(buf.size)
      trans.write(buf)
    end

    def read_message_begin : Tuple(String, UInt8, Int32)
      version = read_i32
      if version < 0
        unsigned_version = version.unsafe_as(UInt32)
        if ((unsigned_version & VERSION_MASK) != VERSION_1)
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "Missing version identifier")
        end
        type = (unsigned_version & TYPE_MASK).to_u8
        name = read_string
        seqid = read_i32
        return name, type, seqid
      else
        if strict_read
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "No version identifier, old protocol client?")
        end
        encoded_name = trans.read_all(version)
        type = read_byte
        seqid = read_i32
        return String.new(encoded_name), type, seqid
      end
    end

    def read_struct_begin; nil; end

    def read_field_begin
      type = read_byte
      if (type == Types::STOP)
        [nil, type, 0]
      else
        id = read_i16
        [nil, type, id]
      end
    end

    def read_map_begin : Tuple(UInt8, UInt8, Int32)
      ktype = read_byte
      vtype = read_byte
      size = read_i32
      return ktype, vtype, size
    end

    def read_list_begin : Tuple(UInt8, Int32)
      etype = read_byte
      size = read_i32
      return etype, size
    end

    def read_set_begin : Tuple(UInt8, Int32)
      etype = read_byte
      size = read_i32
      return etype, size
    end

    def read_bool : Bool
      byte = read_byte
      byte != 0
    end

    def read_byte : UInt8
      val = trans.read_byte
      if (val > 0x7f)
        val = (0 - ((val - 1) ^ 0xff)).to_u8
      end
      val
    end

    def read_i16 : Int16
      trans.read_into_buffer(@rbuf, 2)
      val = IO::ByteFormat::BigEndian.decode(Int16, @rbuf)
    end

    def read_i32 : Int32
      trans.read_into_buffer(@rbuf, 4)
      val = IO::ByteFormat::BigEndian.decode(Int32, @rbuf)
      p! val
      val
    end

    def read_i64 : Int64
      trans.read_into_buffer(@rbuf, 8)
      val = IO::ByteFormat::BigEndian.decode(Int64, @rbuf)
    end

    def read_double : Float64
      trans.read_into_buffer(@rbuf, 8)
      val = IO::ByteFormat::BigEndian.decode(Float64, @rbuf)
    end

    def read_string : String
      buffer = read_binary
      String.new(buffer)
    end

    def read_binary : Bytes
      size = read_i32
      trans.read_all(size)
    end
    
    def to_s
      "binary(#{super.to_s})"
    end
  end

  class BinaryProtocolFactory < BaseProtocolFactory
    def get_protocol(trans)
      return Thrift::BinaryProtocol.new(trans)
    end
    
    def to_s
      "binary"
    end
  end
end