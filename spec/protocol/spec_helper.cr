require "spec"
require "../../src/thrift.cr"

enum TestEnum
  TestValue1
  TestValue2 = 25
  TestValue3
end

class TestClass
  include ::Thrift::Struct

  @[Properties(fid: 1, requirement: :optional)]
  struct_property inst_var1 : Int32?
  @[Properties(fid: 2, requirement: :required)]
  struct_property req_inst_var : String


  def initialize(@req_inst_var, *, inst_var1 = nil)
    inst_var1.try do |inst_var1|
      @inst_var1 = inst_var1
      @__isset.inst_var1 = true
    end
  end
end

class UnionTest
  include ::Thrift::Union

  @[Properties(fid: 1)]
  union_property map : Hash(String, Int32)
  @[Properties(fid: 2)]
  union_property int : Int32
  @[Properties(fid: 3)]
  union_property string : String
  @[Properties(fid: 4)]
  union_property list : Array(Int32)
end
