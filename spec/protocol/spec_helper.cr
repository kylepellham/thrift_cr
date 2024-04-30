require "spec"
require "../../src/thrift.cr"


enum TestEnum
  TestValue1
  TestValue2 = 25
  TestValue3
end

class TestClass
  include ::Thrift::Struct
  @[::Thrift::Struct::Property(id: 1)]
  property inst_var1 : Int32?
  @[::Thrift::Struct::Property(id: 2)]
  property req_inst_var : String?


  def initialize(@inst_var1 = nil, @req_inst_var = nil)
  end

  def validate
    raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN,
                      "Required field req_inst_var is unset!") unless req_inst_var
  end
end

class UnionTest
  include ::Thrift::Union

  @[::Thrift::Struct::Property(id: 1)]
  union_property map : Hash(String, Int32)
  @[::Thrift::Struct::Property(id: 2)]
  union_property int : Int32
  @[::Thrift::Struct::Property(id: 3)]
  union_property string : String
  @[::Thrift::Struct::Property(id: 4)]
  union_property list : Array(Int32)

  def initialize
  end

  def validate
    raise "not set" unless union_set?
  end
end
