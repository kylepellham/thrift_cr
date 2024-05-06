require "uuid"

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

  abstract class BaseProtocol
    getter trans : BaseTransport

    def initialize(@trans)
    end

    abstract def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)

    def write_message_end
      nil
    end

    abstract def write_struct_begin(name)

    def write_struct_end
      nil
    end

    abstract def write_field_begin(name : String, type : Types, id : Int16)

    def write_field_end
      nil
    end

    abstract def write_field_stop

    abstract def write_map_begin(ktype, vtype, size)

    def write_map_end
      nil
    end

    abstract def write_list_begin(etype, size)

    def write_list_end
      nil
    end

    abstract def write_set_begin(etype, size)

    def write_set_end
      nil
    end

    abstract def write_bool(bool : Bool)

    abstract def write_byte(byte : Int8)

    abstract def write_i16(i16 : Int16)

    abstract def write_i32(i32 : Int32)

    abstract def write_i64(i64 : Int64)

    abstract def write_double(dub : Float64)

    abstract def write_uuid(uuid : UUID)

    # Writes a Thrift String. In Crystal 1.5.2 and onward, the String passed will be transcoded to UTF-8.
    #
    # str - The String to write.
    #
    # Raises EncodingError if the transcoding to UTF-8 fails.
    #
    # Returns nothing.
    abstract def write_string(str : String)

    # Writes a Thrift Binary (Thrift String with no encoding). In Crystal 1.5.2 and onward
    #
    # buf - The Bytes to write
    #
    # Returns nothing.
    abstract def write_binary(buf : Bytes)

    abstract def read_message_begin : Tuple(String, Thrift::MessageTypes, Int32)

    def read_message_end
      nil
    end

    abstract def read_struct_begin

    def read_struct_end
      nil
    end

    abstract def read_field_begin : Tuple(String, Thrift::Types, Int32)

    def read_field_end
      nil
    end

    abstract def read_map_begin : Tuple(Thrift::Types, Thrift::Types, Int32)

    def read_map_end
      nil
    end

    abstract def read_list_begin : Tuple(Thrift::Types, Int32)

    def read_list_end
      nil
    end

    abstract def read_set_begin : Tuple(Thrift::Types, Int32)

    def read_set_end
      nil
    end

    abstract def read_bool : Bool

    abstract def read_byte : Int8

    abstract def read_i16 : Int16

    abstract def read_i32 : Int32

    abstract def read_i64 : Int64

    abstract def read_double : Float64

    abstract def read_uuid : UUID

    # Reads a Thrift String. In Crystal 1.5.2 and onward, all Strings will be returned with an Encoding of UTF-8.
    #
    # Returns a String.
    abstract def read_string : String

    # Reads a Thrift Binary (Thrift String without encoding). In  Crystal 1.5.2 and onward, all Strings will be returned
    # with an Encoding of BINARY.
    #
    # Returns a String.
    abstract def read_binary : Bytes

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

# this is where we inject thrifty-ness into crystal
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
  def write(oprot : ::Thrift::BaseProtocol)
  end

  def read(iprot : ::Thrift::BaseProtocol)
    nil
  end
end

struct Bool
  define_thrift_type ::Thrift::Types::Bool

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_bool(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_bool
  end
end

struct Int8 # AKA Byte
  define_thrift_type ::Thrift::Types::Byte

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_byte(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_byte
  end
end

struct Int16
  define_thrift_type ::Thrift::Types::I16

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i16(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_i16
  end
end

struct Int32
  define_thrift_type ::Thrift::Types::I32

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_i32
  end
end

struct Int64
  define_thrift_type ::Thrift::Types::I64

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i64(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_i64
  end
end

struct Float64
  define_thrift_type ::Thrift::Types::Double

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_double(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_double
  end
end

struct UUID
  define_thrift_type ::Thrift::Types::Uuid

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_uuid(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_uuid
  end
end

class String
  define_thrift_type ::Thrift::Types::String

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_string(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_string
  end
end

# Bytes is aliased as Slice(UInt8) so we throw on any slice that isn't bytes
struct Slice(T)
  def write(oprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars.size < 2 && @type.type_vars[0].name.stringify == "UInt8" %}
      oprot.write_binary(self)
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not UInt8"
    {% end %}
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars.size < 2 && @type.type_vars[0].name.stringify == "UInt8" %}
      iprot.read_binary
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not UInt8"
    {% end %}
  end
end

abstract struct Enum
  define_thrift_type ::Thrift::Types::I32

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self.to_i32)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    self.from_value?(iprot.read_i32)
  end
end

# for container types we don't have write methods for non-thrift types

class Array(T)
  define_thrift_type ::Thrift::Types::List

  def write(oprot : ::Thrift::BaseProtocol)
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

  def self.read(iprot : ::Thrift::BaseProtocol)
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

  def write(oprot : ::Thrift::BaseProtocol)
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

  def self.read(iprot : ::Thrift::BaseProtocol)
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

  def write(oprot : ::Thrift::BaseProtocol)
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

  def self.read(iprot : ::Thrift::BaseProtocol)
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
