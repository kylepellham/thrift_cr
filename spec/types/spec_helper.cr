require "spec"
require "../../src/thrift.cr"

class TestStruct
  include ::Thrift::Struct

  @required_fields__{{@type.name.id}} = {"field2" => :unset}
  struct_property field1 : Int32?
  struct_property field2 : String
  struct_property field3 : Array(Int32)

  def initialize(@field2, @field1 = nil, @field3 = nil)
    @required_fields__{{@type.name.id}}["field2"] = :set
  end

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_recursion do
      {% begin %}
      if !(%empty_fields = @required_fields__{{@type.name.id}}.select{|k,v| v == :unset})).empty?
        raise ::Thrift::ProtocolException.new ::Thrift::ProtocolException::UNKNOWN, "Fields Empty when writing #{%empty_fields.keys}"
      end
      {% end %}
      oprot.write_struct_begin("TestStruct")
      @field1.try do |field1|
        oprot.write_field_begin("field1", Int32::THRIFT_TYPE, 0_i16)
        field1.write to: oprot
        oprot.write_field_end
      end

      @field2.try do |field2|
        oprot.write_field_begin "field2", String::THRIFT_TYPE, 1_i16
        field2.write to: oprot
        oprot.write_field_end
      end

      @field3.try do |field3|
        oprot.write_field_begin("field3", Array::THRIFT_TYPE), 2_i16
        field3.write to: oprot
        oprot.write_field_end
      end

      oprot.write_field_stop
      oprot.write_struct_end
    end
  end

  def read(from iprot : ::Thrift::BaseProtocol)
    {% begin %}
    iprot.read_recursion do
      %required_fields_set = {"field2" => false}
      iprot.read_struct_begin

      loop do
        name, ftype, fid = iprot.read_field_begin
        break if ftype == ::Thrift::Types::Stop
        case {fid, ftype}
        when {0, Int32::THRIFT_TYPE}
          @field1 = Int32.read from: iprot
        when {1, String::THRIFT_TYPE}
          @field2 = String.read from: iprot
          %required_fields["field2"] = true
        when {2, Array::THRIFT_TYPE}
          @field3 = Array(Int32).read from: iprot
        else
          iprot.skip(ftype)
        end
        iprot.read_field_end
      end
      iprot.read_struct_end
      %required_fields_set.select!{|k,v| !v}
      raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, "Required field(s) were not set: #{%required_fields_set.keys}") unless %required_fields_set.empty?
    end
    {% end %}
  end
end