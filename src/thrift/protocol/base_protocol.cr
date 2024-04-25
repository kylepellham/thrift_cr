require "../types.cr"
require "../transport/base_transport.cr"

module Thrift
  class ProtocolException < Exception
    UNKNOWN         = 0
    INVALID_DATA    = 1
    NEGATIVE_SIZE   = 2
    SIZE_LIMIT      = 3
    BAD_VERSION     = 4
    NOT_IMPLEMENTED = 5
    DEPTH_LIMIT     = 6

    getter :type

    def initialize(@type = UNKNOWN, message = nil)
      super(message)
    end
  end

  class BaseProtocol
    getter trans : BaseTransport

    def initialize(@trans)
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      raise NotImplementedError.new ""
    end

    def write_message_end
      nil
    end

    def write_struct_begin(name)
      raise NotImplementedError.new ""
    end

    def write_struct_end
      nil
    end

    def write_field_begin(name : String, type : Types, id : Int16)
      raise NotImplementedError.new ""
    end

    def write_field_end
      nil
    end

    def write_field_stop
      raise NotImplementedError.new ""
    end

    def write_map_begin(ktype, vtype, size)
      raise NotImplementedError.new ""
    end

    def write_map_end
      nil
    end

    def write_list_begin(etype, size)
      raise NotImplementedError.new ""
    end

    def write_list_end
      nil
    end

    def write_set_begin(etype, size)
      raise NotImplementedError.new ""
    end

    def write_set_end
      nil
    end

    def write_bool(bool : Bool)
      raise NotImplementedError.new ""
    end

    def write_byte(byte : Int8)
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

    # Writes a Thrift String. In Crystal 1.5.2 and onward, the String passed will be transcoded to UTF-8.
    #
    # str - The String to write.
    #
    # Raises EncodingError if the transcoding to UTF-8 fails.
    #
    # Returns nothing.
    def write_string(str : String)
      raise NotImplementedError.new ""
    end

    # Writes a Thrift Binary (Thrift String with no encoding). In Crystal 1.5.2 and onward
    #
    # buf - The Bytes to write
    #
    # Returns nothing.
    def write_binary(buf : Bytes)
      raise NotImplementedError.new ""
    end

    def read_message_begin : Tuple(String, Thrift::MessageTypes, Int32)
      raise NotImplementedError.new ""
    end

    def read_message_end
      nil
    end

    def read_struct_begin
      raise NotImplementedError.new ""
    end

    def read_struct_end
      nil
    end

    def read_field_begin : Tuple(String, Thrift::Types, Int32)
      raise NotImplementedError.new ""
    end

    def read_field_end
      nil
    end

    def read_map_begin : Tuple(Thrift::Type, Thrift::Type, Int32)
      raise NotImplementedError.new ""
    end

    def read_map_end
      nil
    end

    def read_list_begin : Tuple(Thrift::Type, Int32)
      raise NotImplementedError.new ""
    end

    def read_list_end
      nil
    end

    def read_set_begin : Tuple(Thrift::Type, Int32)
      raise NotImplementedError.new ""
    end

    def read_set_end
      nil
    end

    def read_bool : Bool
      raise NotImplementedError.new ""
    end

    def read_byte : Int8
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

    # Reads a Thrift String. In Crystal 1.5.2 and onward, all Strings will be returned with an Encoding of UTF-8.
    #
    # Returns a String.
    def read_string : String
      raise NotImplementedError.new ""
    end

    # Reads a Thrift Binary (Thrift String without encoding). In  Crystal 1.5.2 and onward, all Strings will be returned
    # with an Encoding of BINARY.
    #
    # Returns a String.
    def read_binary : Bytes
      raise NotImplementedError.new ""
    end

    def skip(type)
      case type
      when Types::Bool
        read_bool
      when Types::Byte
        read_byte
      when Types::I16
        read_i16
      when Types::I32
        read_i32
      when Types::I64
        read_i64
      when Types::Double
        read_double
      when Types::String
        read_string
      when Types::Struct
        read_struct_begin
        while true
          name, type, id = read_field_begin
          break if type == Types::Stop
          skip(type)
          read_field_end
        end
        read_struct_end
      when Types::Map
        ktype, vtype, size = read_map_begin
        size.times do
          skip(ktype)
          skip(vtype)
        end
        read_map_end
      when Types::Set
        etype, size = read_set_begin
        size.times do
          skip(etype)
        end
        read_set_end
      when Types::List
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
    def get_protocol(trans) : ::Thrift::BaseProtocol
      raise NotImplementedError.new ""
    end

    def to_s
      "base"
    end
  end
end

# this where we inject thrifty-ness into crystal
macro define_thrift_type(thrift_type)
  def self.thrift_type
    {{thrift_type}}
  end

  def thrift_type
    \{{@type}}.thrift_type
  end
end

struct Nil
  define_thrift_type ::Thrift::Types::Void

  # we only need a write to accomidate nilable types
  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
  end

  def read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    nil
  end
end

struct Bool
  define_thrift_type ::Thrift::Types::Bool

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_bool(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_bool
  end
end

struct Int8 # AKA Byte
  define_thrift_type ::Thrift::Types::Byte

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_byte(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    iprot.read_byte
  end
end

struct Int16
  define_thrift_type ::Thrift::Types::I16

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_i16(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    iprot.read_i16
  end
end

struct Int32
  define_thrift_type ::Thrift::Types::I32

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_i32(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    iprot.read_i32
  end
end

struct Int64
  define_thrift_type ::Thrift::Types::I64

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_i64(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    iprot.read_i64
  end
end

struct Float64
  define_thrift_type ::Thrift::Types::Double

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_double(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    iprot.read_double
  end
end

class String
  define_thrift_type ::Thrift::Types::String

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    if  kwargs[:binary]?
      oprot.write_binary(self.to_slice)
    else
      oprot.write_string(self)
    end
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    if kwargs[:binary]?
      String.new(iprot.read_binary)
    else
      iprot.read_string
    end
  end
end

abstract struct Enum
  define_thrift_type ::Thrift::Types::I32

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    oprot.write_i32(self.to_i32)
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    self.from_value?(iprot.read_i32)
  end
end

# for container types we don't have write methods for non-thrift types

class Array(T)
  define_thrift_type ::Thrift::Types::List

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      oprot.write_list_begin(T.thrift_type, self.size)
      each do |element|
        element.write(oprot)
      end
      oprot.write_list_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      etype, size = iprot.read_list_begin
      raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, "Recived Type doesn't match - recieved: #{::Thrift::Types.from_value?(etype)}, expected: #{T.thrift_type}") if T.thrift_type != ::Thrift::Types.from_value?(etype)
      ret = Array(T).new(size) do |_|
        T.read(iprot)
      end
      iprot.read_list_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end

struct Set(T)
  define_thrift_type ::Thrift::Types::Set

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      oprot.write_set_begin(T.thrift_type, self.size)
      each do |element|
        element.write(oprot)
      end
      oprot.write_set_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    ret = Set(T).new
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      etype, size = iprot.read_set_begin
      raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, "Recived Type doesn't match - recieved: #{::Thrift::Types.from_value?(etype)}, expected: #{T.thrift_type}") if T.thrift_type != ::Thrift::Types.from_value?(etype)
      size.times do |_|
        ret << T.read(iprot)
      end
      iprot.read_set_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end

class Hash(K, V)
  define_thrift_type ::Thrift::Types::Map

  def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) &&
          @type.type_vars[1].class.has_method?(:thrift_type) %}
      oprot.write_map_begin(K.thrift_type, V.thrift_type, self.size)
      each do |key, value|
        key.write(oprot)
        value.write(oprot)
      end
      oprot.write_map_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} OR/AND {{@type.type_vars[1].instance}} is not a thrift type"
    {% end %}
    {% debug %}
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *args, **kwargs)
    ret = Hash(K, V).new
    {% if @type.type_vars[0].class.has_method?(:thrift_type) &&
          @type.type_vars[1].class.has_method?(:thrift_type) %}
      ktype, vtype, size = iprot.read_map_begin
      if (ktype_enum = ::Thrift::Types.from_value?(ktype)) != K.thrift_type ||
         (vtype_enum = ::Thrift::Types.from_value?(vtype)) != V.thrift_type
        message = "Recieved type for Keys AND/OR Value do not match - expected key: #{K.thrift_type}, recieved key: #{ktype_enum} - expected value: #{V.thrift_type}, recieved value: #{vtype_enum}"
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, message)
      end
      size.times do |_|
        key = K.read(iprot)
        ret[key] = V.read(iprot)
      end
      iprot.read_map_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end
