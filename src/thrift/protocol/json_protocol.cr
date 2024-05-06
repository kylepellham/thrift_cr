require "base64"
require "json"
require "./base_protocol.cr"

module Thrift

  class JsonProtocol < BaseProtocol
    THRIFT_VERSION1 = 1

    # if we haven't read before create a pullparser and try again
    # use "reader" to access the pullparser
    private macro handle_read(&)
      if reader = @reader
        {{yield}}
      else
        @reader = JSON::PullParser.new(@trans)
        \{{@def.name}}
      end
    end

    private macro handle_write_begin(&)
      if @started_documents < 1
        writer.start_document
      end
      @started_documents += 1
      {{yield}}
      nil
    end

    private macro handle_write_end(&)
      {{yield}}
      if @started_documents < 2
        writer.end_document
      end
      @started_documents -= 1
      nil
    end

    getter writer : JSON::Builder
    @reader : JSON::PullParser?
    @started_documents = 0
    def initialize(trans)
      super(trans)
      # we get to take advantage of the fact transport inherits from IO for this
      @writer = JSON::Builder.new(trans)
    end

    private macro handle_write(&)
      if @started_documents < 1
        writer.start_document
      end
      @started_documents += 1
      ret = {{yield}}
      if @started_documents < 2
        writer.end_document
      end
      @started_documents -= 1
      ret
    end

    def reset_ios
      if 0 < @started_documents
        writer.end_document
        @started_documents = 0
      end
      begin
        @reader = nil
      rescue
      end
    end

    def get_json_name_from_type(ttype)
      case ttype
      when Types::Bool
        "tf"
      when Types::Byte
        "i8"
      when Types::I16
        "i16"
      when Types::I32
        "i32"
      when Types::I64
        "i64"
      when Types::Double
        "dbl"
      when Types::String
        "str"
      when Types::Struct
        "rec"
      when Types::Map
        "map"
      when Types::Set
        "set"
      when Types::List
        "lst"
      else
        raise NotImplementedError.new ""
      end
    end

    def get_type_from_json_name(name)
      case name
      when "tf"
        result = Types::Bool
      when "i8"
        result = Types::Byte
      when "i16"
        result = Types::I16
      when "i32"
        result = Types::I32
      when "i64"
        result = Types::I64
      when "dbl"
        result = Types::Double
      when "str"
        result = Types::String
      when "rec"
        result = Types::Struct
      when "map"
        result = Types::Map
      when "set"
        result = Types::Set
      when "lst"
        result = Types::List
      else
        result = Types::Stop
      end
      if (result == Types::Stop)
        raise NotImplementedError.new ""
      end
      return result
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      handle_write_begin do
        writer.start_array
        writer.number(THRIFT_VERSION1)
        writer.string(name)
        writer.number(type.to_i)
        writer.number(seqid)
      end
    end

    def write_message_end
      handle_write_end do
        writer.end_array
      end
    end

    def write_struct_begin(name)
      handle_write_begin do
        writer.start_object
      end
    end

    def write_struct_end
      handle_write_end do
        writer.end_object
      end
    end

    def write_field_begin(name : String, type : Thrift::Types, id : Int16)
      handle_write_begin do
        writer.string(id)
        writer.start_object
        writer.string(get_json_name_from_type(type))
      end
    end

    def write_field_end
      handle_write_end do
        writer.end_object
      end
    end

    def write_field_stop
      nil
    end

    def write_map_begin(ktype, vtype, size)
      handle_write_begin do
        writer.start_array
        writer.string(get_json_name_from_type(ktype))
        writer.string(get_json_name_from_type(vtype))
        writer.number(size)
        writer.start_object
      end
    end

    def write_map_end
      handle_write_end do
        writer.end_object
        writer.end_array
      end
    end

    def write_list_begin(etype, size)
      handle_write_begin do
        writer.start_array
        writer.string(get_json_name_from_type(etype))
        writer.number(size)
      end
    end

    def write_list_end
      handle_write_end do
        writer.end_array
      end
    end

    def write_set_begin(etype, size)
      write_list_begin(etype, size)
    end

    def write_set_end
      write_list_end
    end

    def write_bool(bool : Bool)
      handle_write do
        writer.number(bool ? 1 : 0)
      end
    end

    def write_byte(byte : Int8)
      handle_write do
        writer.number(byte)
      end
    end

    def write_i16(i16 : Int16)
      handle_write do
        writer.number(i16)
      end
    end

    def write_i32(i32 : Int32)
      handle_write do
        writer.number(i32)
      end
    end

    def write_i64(i64 : Int64)
      handle_write do
        writer.number(i64)
      end
    end

    def write_double(dub : Float64)
      handle_write do
        writer.number(dub)
      end
    end

    def write_uuid(uuid : UUID)
      handle_write do
        writer.string uuid
      end
    end

    def write_string(str : String)
      handle_write do
        writer.scalar(str)
      end
    end

    def write_binary(buf : Bytes)
      handle_write do
        writer.scalar(Base64.encode(buf))
      end
    end

    def read_message_begin : Tuple(String, Thrift::MessageTypes, Int32)
      handle_read do
        reader.read_begin_array
        version = reader.read_int
        if version != THRIFT_VERSION1
          raise ProtocolException.new ProtocolException::BAD_VERSION, "message contained bad version"
        end
        name = reader.read_string
        message_type = Thrift::MessageTypes.new(reader.read_int.to_i8)
        seqid = reader.read_int.to_i32

        return name, message_type, seqid
      end
    end

    def read_message_end
      reader.read_end_array
      nil
    end

    def read_struct_begin
      handle_read do
        reader.read_begin_object
      end
    end

    def read_struct_end
      reader.read_end_object
      nil
    end

    def read_field_begin : Tuple(String, Thrift::Types, Int32)
      handle_read do
        kind = reader.kind
        if kind == JSON::PullParser::Kind::EndObject
          fid = 0
          ftype = Types::Stop
        else
          fid = reader.read_int.to_i32
          reader.read_begin_object
          temp = reader.read_string
          ftype = get_type_from_json_name(temp)
        end
        return "", ftype, fid
      end
    end

    def read_field_end
      reader.read_end_object
    end

    def read_map_begin : Tuple(Thrift::Types, Thrift::Types, Int32)
      handle_read do
        reader.read_begin_array
        ktype = get_type_from_json_name(reader.read_string)
        vtype = get_type_from_json_name(reader.read_string)
        size = reader.read_int
        reader.read_begin_object

        return ktype, vtype, size
      end
    end

    def read_map_end
      reader.read_end_object
      reader.read_end_array
    end

    def read_list_begin : Tuple(Thrift::Types, Int32)
      handle_read do
        reader.read_begin_array
        etype = get_type_from_json_name(reader.read_string)
        size = reader.read_int.to_i32

        return etype, size
      end
    end

    def read_list_end
      handle_read do
        reader.read_end_array
      end
    end

    def read_set_begin : Tuple(Thrift::Types, Int32)
      read_list_begin
    end

    def read_set_end
      read_list_end
    end

    def read_bool : Bool
      read_byte != 0
    end

    def read_byte : Int8
      handle_read do
        reader.read_int.to_i8
      end
    end

    def read_i16 : Int16
      handle_read do
        reader.read_int.to_i16
      end
    end

    def read_i32 : Int32
      handle_read do
        reader.read_int.to_i32
      end
    end

    def read_i64 : Int64
      handle_read do
        reader.read_int
      end
    end

    def read_double : Float64
      handle_read do
        reader.read_float
      end
    end

    def read_uuid : UUID
      handle_read do
        UUID.new reader.read_string
      end
    end

    def read_string : String
      handle_read do
        reader.read_string
      end
    end

    def read_binary : Bytes
      handle_read do
        Base64.decode(reader.read_string)
      end
    end
  end

  class JsonProtocolFactory < BaseProtocolFactory
    def get_protocol(trans)
      ::Thrift::JsonProtocol.new(trans)
    end

    def to_s
      "json"
    end
  end
end