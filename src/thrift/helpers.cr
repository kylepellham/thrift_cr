require "./types.cr"
require "./protocol/base_protocol.cr"

struct Slice(T)
  def join_with(other : self)
    new_slice = Pointer(T).malloc self.size + other.size
    appender = new_slice.appender
    self.each do |element|
      appender << element
    end
    other.each do |element|
      appender << element
    end
    Slice.new(new_slice, appender.size)
  end

  def << (other : self)
    join_with(other)
  end
end

macro define_thrift_type(thrift_type)
  def self.thrift_type
    {{thrift_type}}
  end

  def thrift_type
    \{{@type}}.thrift_type
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

struct Int8
  define_thrift_type ::Thrift::Types::I8

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    iprot.read_i8
  end
end

struct Int16
  define_thrift_type ::Thrift::Types::I16

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self)
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

class String
  define_thrift_type ::Thrift::Types::String

  def write(oprot : ::Thrift::BaseProtocol, *, binary = false)
    if binary
      oprot.write_binary(self.encode("utf-8"))
    else
      oprot.write_string(self)
    end
  end

  def self.read(iprot : ::Thrift::BaseProtocol, *, binary = false)
    if binary
      iprot.read_binary.hexstring
    else
      iprot.read_string
    end
  end
end

abstract struct Enum
  define_thrift_type ::Thrift::Types::I32

  def write(oprot : ::Thrift::BaseProtocol)
    oprot.write_i32(self.to_i32)
  end

  def self.read(iprot : ::Thrift::BaseProtocol)
    self.from_value(iprot.read_i32)
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
      _, size = iprot.read_list_begin
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
      _, size = iprot.read_set_begin
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
      oprot.write_map_begin(K.thrift_type, V.thrift_type, self. size)
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
          @type.type_vars[1].class.has_method?(:thrift_type)%}
      _, _, size = iprot.read_map_begin
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
