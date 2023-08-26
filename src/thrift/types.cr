module Thrift
  enum Types : UInt8
    STOP = 0
    VOID = 1
    BOOL = 2
    BYTE = 3
    DOUBLE = 4
    I16 = 6
    I32 = 8
    I64 = 10
    STRING = 11
    STRUCT = 12
    MAP = 13
    SET = 14
    LIST = 15
  end

  def self.check_type(value, field, name, skip_nil=true)
    return if value.nil? && skip_nil
    klasses = case field[:type]
              when Types::VOID
                Nil
              when Types::BOOL
                Boolean
              when Types::BYTE, Types::I16, Types::I32, Types::I64
                Int
              when Types::DOUBLE
                Float64
              when Types::STRING
                String
              when Types::STRUCT
                [Struct, Union]
              when Types::MAP
                Hash
              when Types::SET
                Set
              when Types::LIST
                Array
              end
    valid = klasses && klasses.any? { |klass| klass == value }
    raise TypeError, "Expected #{type_name(field[:type])}, received #{value.class} for field #{name}" unless valid
    # check elements now
    case field[:type]
    when Types::MAP
      value.each_pair do |k,v|
        check_type(k, field[:key], "#{name}.key", false)
        check_type(v, field[:value], "#{name}.value", false)
      end
    when Types::SET, Types::LIST
      value.each do |el|
        check_type(el, field[:element], "#{name}.element", false)
      end
    when Types::STRUCT
      raise TypeError, "Expected #{field[:class]}, received #{value.class} for field #{name}" unless field[:class] == value.class
    end
  end

  def self.type_name(type)
    Types.constants.each do |const|
      return "Types::#{const}" if Types.const_get(const) == type
    end
    nil
  end

  enum MessageTypes : UInt8
    CALL = 1
    REPLY = 2
    EXCEPTION = 3
    ONEWAY = 4
  end
end

