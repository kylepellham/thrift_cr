require "./base_protocol.cr"

module Thrift
  class BinaryProtocol < BaseProtocol
    VERSION_MASK = 0xffff0000_u32
    VERSION_1    = 0x80010000_u32
    TYPE_MASK    = 0x000000ff_u32

    getter :strict_read, :strict_write
    getter byte_format : IO::ByteFormat

    def initialize(trans, @strict_read = true, @strict_write = true, *, @byte_format = IO::ByteFormat::NetworkEndian)
      super(trans)
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      # this is necessary because we added (needed) bounds checking to
      # write_i32, and 0x80010000 is too big for that.
      if strict_write
        write_i16((0xffff_u16 & (VERSION_1 >> 16)).unsafe_as(Int16))
        write_i16(type.to_i16)
        write_string(name)
        write_i32(seqid)
      else
        write_string(name)
        write_byte(type.to_i8)
        write_i32(seqid)
      end
    end

    def write_struct_begin(name)
      nil
    end

    def write_field_begin(name : String, type : Types, id : Int16)
      write_byte(type.to_i8)
      write_i16(id)
    end

    def write_field_stop
      write_byte(Thrift::Types::Stop.to_i8)
    end

    def write_map_begin(ktype, vtype, size)
      write_byte(ktype.to_i8)
      write_byte(vtype.to_i8)
      write_i32(size)
    end

    def write_list_begin(etype, size)
      write_byte(etype.to_i8)
      write_i32(size)
    end

    def write_set_begin(etype, size)
      write_byte(etype.to_i8)
      write_i32(size)
    end

    def write_bool(bool)
      write_byte(bool ? 1_i8 : 0_i8)
    end

    def write_byte(byte : Int8)
      trans.write_bytes(byte, byte_format)
    end

    def write_i16(i16 : Int16)
      trans.write_bytes(i16, byte_format)
    end

    def write_i32(i32 : Int32)
      trans.write_bytes(i32, byte_format)
    end

    def write_i64(i64 : Int64)
      trans.write_bytes(i64, byte_format)
    end

    def write_double(dub : Float64)
      trans.write_bytes(dub, byte_format)
    end

    def write_uuid(uuid : UUID)
      trans.write(uuid.bytes.to_slice)
    end

    def write_string(str : String)
      buf = str.encode("utf-8")
      write_binary(buf)
    end

    def write_binary(buf : Bytes)
      write_i32(buf.size)
      trans.write(buf)
    end

    def read_message_begin : Tuple(String, Thrift::MessageTypes, Int32)
      version = read_i32
      if version < 0
        unsigned_version = version.unsafe_as(UInt32)
        if ((unsigned_version & VERSION_MASK) != VERSION_1)
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "Missing version identifier")
        end
        type = Thrift::MessageTypes.new(unsigned_version.unsafe_as(Int8))
        name = read_string
        seqid = read_i32
        return name, type, seqid
      else
        if strict_read
          raise ProtocolException.new(ProtocolException::BAD_VERSION, "No version identifier, old protocol client?")
        end
        encoded_name = Bytes.new(version)
        trans.read(encoded_name)
        # encoded_name = trans.read_all(version)
        type = Thrift::MessageTypes.new(read_byte)
        seqid = read_i32
        return String.new(encoded_name), type, seqid
      end
    end

    def read_struct_begin
      nil
    end

    def read_field_begin : Tuple(String, Thrift::Types, Int16)
      type = Types.from_value(read_byte)
      if (type == Types::Stop)
        {"", type, 0_i16}
      else
        id = read_i16
        {"", type, id}
      end
    end

    def read_map_begin : Tuple(Thrift::Types, Thrift::Types, Int32)
      ktype = Thrift::Types.from_value(read_byte)
      vtype = Thrift::Types.from_value(read_byte)
      size = read_i32
      return ktype, vtype, size
    end

    def read_list_begin : Tuple(Thrift::Types, Int32)
      etype = Thrift::Types.from_value(read_byte)
      size = read_i32
      return etype, size
    end

    def read_set_begin : Tuple(Thrift::Types, Int32)
      etype = Thrift::Types.new(read_byte)
      size = read_i32
      return etype, size
    end

    def read_bool : Bool
      byte = read_byte
      byte != 0
    end

    def read_byte : Int8
      byte = trans.read_byte
      raise ProtocolException.new ProtocolException::INVALID_DATA, "Not enought Bytes to read" unless byte
      byte.unsafe_as(Int8)
    end

    def read_i16 : Int16
      trans.read_bytes(Int16, byte_format)
    end

    def read_i32 : Int32
      trans.read_bytes(Int32, byte_format)
    end

    def read_i64 : Int64
      trans.read_bytes(Int64, byte_format)
    end

    def read_double : Float64
      trans.read_bytes(Float64, byte_format)
    end

    def read_uuid : UUID
      uuid_bytes = Bytes.new(16)
      bytes_read = trans.read(uuid_bytes)
      UUID.new uuid_bytes
    end

    def read_string : String
      size = read_i32
      bytes = Bytes.new(size)
      trans.read(bytes)
      String.new(bytes, "utf-8")
    end

    def read_binary : Bytes
      size = read_i32
      bytes = Bytes.new(size)
      trans.read(bytes)
      bytes
    end

    def to_s
      "binary(#{super.to_s})"
    end
  end

  class BinaryProtocolFactory < BaseProtocolFactory
    getter byte_format : IO::ByteFormat
    def initialize(@byte_format = IO::ByteFormat::BigEndian)
    end

    def get_protocol(trans)
      return Thrift::BinaryProtocol.new(trans, byte_format: byte_format)
    end

    def to_s
      "binary"
    end
  end
end
