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
    RECURSION_LIMIT = 64
    @read_recursion = 0
    @write_recursion = 0

    def read_recursion(&)
      @read_recursion += 1
      raise ProtocolException.new ProtocolException::DEPTH_LIMIT, "reached max read depth of #{ProtocolException::DEPTH_LIMIT}" if @read_recursion >= RECURSION_LIMIT
      yield self
    ensure
      @read_recursion -= 1
    end

    def write_recursion(&)
      @write_recursion += 1
      raise ProtocolException.new ProtocolException::DEPTH_LIMIT, "reached max write depth of #{ProtocolException::DEPTH_LIMIT}" if @write_recursion >= RECURSION_LIMIT
      yield self
    ensure
      @write_recursion -= 1
    end

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

    abstract def read_field_begin : Tuple(String, Thrift::Types, Int16)

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
        loop do
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

struct Nil
  include ::Thrift::Type

  define_thrift_type ::Thrift::Types::Void

  # we only need a write to accomidate nilable types
  def write(to oprot : ::Thrift::BaseProtocol)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    nil
  end
end

struct Bool
  include ::Thrift::Type

  define_thrift_type ::Thrift::Types::Bool

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_bool(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_bool
  end
end

struct Int8 # AKA Byte
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::Byte

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_byte(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_byte
  end
end

struct Int16
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::I16

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_i16(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_i16
  end
end

struct Int32
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::I32

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_i32
  end
end

struct Int64
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::I64

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_i64(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_i64
  end
end

struct Float64
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::Double

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_double(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_double
  end
end

struct UUID
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::Uuid

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_uuid(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_uuid
  end
end

class String
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::String

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_string(self)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_string
  end
end

# Bytes is aliased as Slice(UInt8) so we throw on any slice that isn't bytes
struct Slice(T)
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::String

  def write(to oprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars.size < 2 && @type.type_vars[0] == UInt8 %}
      oprot.write_binary(self)
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not UInt8"
    {% end %}
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars.size < 2 && @type.type_vars[0] == UInt8 %}
      iprot.read_binary
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not UInt8"
    {% end %}
  end
end

abstract struct Enum
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::I32

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self.to_i32)
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    self.from_value(iprot.read_i32)
  end
end

# for container types we don't have write methods for non-thrift types

class Array(T)
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::List

  def write(to oprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      oprot.write_list_begin(T.thrift_type, self.size)
      each do |element|
        element.write to: oprot
      end
      oprot.write_list_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      etype, size = iprot.read_list_begin
      raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, "Recived Type doesn't match - recieved: #{etype}, expected: #{T.thrift_type}") if T.thrift_type != etype
      ret = Array(T).new(size) do |_|
        T.read from: iprot
      end
      iprot.read_list_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end

struct Set(T)
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::Set

  def write(to oprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      oprot.write_set_begin(T.thrift_type, self.size)
      each do |element|
        element.write to: oprot
      end
      oprot.write_set_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    ret = Set(T).new
    {% if @type.type_vars[0].class.has_method?(:thrift_type) %}
      etype, size = iprot.read_set_begin
      raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, "Recived Type doesn't match - recieved: #{etype}, expected: #{T.thrift_type}") if T.thrift_type != etype
      size.times do |_|
        ret << T.read from: iprot
      end
      iprot.read_set_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end

class Hash(K, V)
  include ::Thrift::Type
  define_thrift_type ::Thrift::Types::Map

  def write(to oprot : ::Thrift::BaseProtocol)
    {% if @type.type_vars[0].class.has_method?(:thrift_type) &&
          @type.type_vars[1].class.has_method?(:thrift_type) %}
      oprot.write_map_begin(K.thrift_type, V.thrift_type, self.size)
      each do |key, value|
        key.write to: oprot
        value.write to: oprot
      end
      oprot.write_map_end
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} OR/AND {{@type.type_vars[1].instance}} is not a thrift type"
    {% end %}
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
    ret = Hash(K, V).new
    {% if @type.type_vars[0].class.has_method?(:thrift_type) &&
          @type.type_vars[1].class.has_method?(:thrift_type) %}
      ktype, vtype, size = iprot.read_map_begin
      if (ktype_enum = ktype) != K.thrift_type ||
        (vtype_enum = vtype) != V.thrift_type
        message = "Recieved type for Keys AND/OR Value do not match - expected key: #{K.thrift_type}, recieved key: #{ktype_enum} - expected value: #{V.thrift_type}, recieved value: #{vtype_enum}"
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::INVALID_DATA, message)
      end
      size.times do |_|
        ret[K.read from: iprot] = V.read from: iprot
      end
      iprot.read_map_end
      ret
    {% else %}
      raise NotImplementedError.new "{{@type.type_vars[0].instance}} is not a thrift type"
    {% end %}
  end
end
