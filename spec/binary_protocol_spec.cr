require "./spec_helper.cr"

class TestClass
  @[::Thrift::Struct::Property(id: 1)]
  property inst_var1 : Int32

  def initialize(@inst_var1)
  end
end