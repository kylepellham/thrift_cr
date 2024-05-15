require "spec"
require "../../src/thrift.cr"


enum TestEnum
  TestValue1
  TestValue2 = 25
  TestValue3
end

class TestClass
  include ::Thrift::Struct
  @[::Thrift::Struct::Property(id: 1, req_in: false, req_out: false)]
  struct_property inst_var1 : Int32?
  @[::Thrift::Struct::Property(id: 2, req_in: true, req_out: true)]
  struct_property req_inst_var : String


  def initialize(@req_inst_var, @inst_var1 = nil)
  end

  def validate
    raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN,
                      "Required field req_inst_var is unset!") if @req_inst_var.nil?
  end

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_struct_begin("TestClass")

    @inst_var1.try do |inst_var1|
      oprot.write_field_begin("inst_var1", Int32.thrift_type, 1_i16)
      inst_var1.write to: oprot
      oprot.write_field_end
    end

    @req_inst_var.try do |req_inst_var|
      oprot.write_field_begin("req_inst_var", String.thrift_type, 2_i16)
      req_inst_var.write to: oprot
      oprot.write_field_end
    end

    oprot.write_field_stop
    oprot.write_struct_end
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
  end
end

class UnionTest
  include ::Thrift::Union

  union_property map : Hash(String, Int32)
  union_property int : Int32
  union_property string : String
  union_property list : Array(Int32)

  def initialize
  end

  def validate
    raise "not set" unless union_set?
  end

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_struct_begin("UnionTest")
    case union_internal
    when .is_a?(Hash(String, Int32))
      oprot.write_field_begin("map", Hash.thrift_type, 1_i16)
      map.write to: oprot
      oprot.write_field_end
    when .is_a?(Int32)
      oprot.write_field_begin("int", Int32.thrift_type, 2_i16)
      int.write to: oprot
      oprot.write_field_end
    when .is_a?(String)
      oprot.write_field_begin("string", String.thrift_type, 3_i16)
      string.write to: oprot
      oprot.write_field_end
    when.is_a?(Array(Int32))
      oprot.write_field_begin("list", Array.thrift_type, 4_i16)
      list.write to: oprot
      oprot.write_field_end
    end
    oprot.write_field_stop
    oprot.write_struct_end
  end

  def self.read(from iprot : ::Thrift::BaseProtocol)
  end
end
