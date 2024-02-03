require "./spec_helper.cr"

class TestClass
  @[::Thrift::Struct::Property(id: 1)]
  property inst_var1 : Int32?

  def initialize(@inst_var1)
  end
end

binary_serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new)

describe ::Thrift::BinaryProtocol do
  describe "#write_i8" do
    it "writes Int8" do
      binary_serializer.serialize(12_i8).should eq(Bytes[12])
    end

    it "writes max Int8" do
      binary_serializer.serialize(Int8::MAX).should eq(Bytes[127])
    end

    it "writes negative Int8" do
      binary_serializer.serialize(-1_i8).should eq(Bytes[255])
    end
  end

  describe "#write_i16" do
    it "writes Int16" do
      binary_serializer.serialize(256_i16).should eq(Bytes[1, 0])
    end

    it "writes Max Int16" do
      binary_serializer.serialize(Int16::MAX).should eq(Bytes[127, 255])
    end

    it "writes negative Int16" do
      binary_serializer.serialize(-1_i16).should eq(Bytes[255, 255])
    end
  end

  describe "#write_i32" do
    it "writes Int32" do
      binary_serializer.serialize(650).should eq(Bytes[0, 0, 2, 138])
    end

    it "writes Max Int32" do
      binary_serializer.serialize(Int32::MAX).should eq(Bytes[127, 255, 255, 255])
    end

    it "writes negative Int32" do
      binary_serializer.serialize(-1).should eq(Bytes[255, 255, 255, 255])
    end
  end

  describe "#write_i64" do
    it "writes Int64" do
      binary_serializer.serialize(4611686018427387903_i64).should eq(Bytes[63, 255, 255, 255, 255, 255, 255, 255])
    end

    it "writes Max Int64" do
      binary_serializer.serialize(Int64::MAX).should eq(Bytes[127, 255, 255, 255, 255, 255, 255, 255])
    end

    it "writes negative Int64" do
      binary_serializer.serialize(-1_i64).should eq(Bytes[255, 255, 255, 255, 255, 255, 255, 255])
    end
  end

  describe "#write_double" do
    it "writes double" do
      binary_serializer.serialize(3535.25523219003436734).should eq(Bytes[64, 171, 158, 130, 173, 203, 42, 43])
    end

    it "writes Max double" do
      binary_serializer.serialize(Float64::MAX).should eq(Bytes[127, 239, 255, 255, 255, 255, 255, 255])
    end

    it "writes negative double" do
      binary_serializer.serialize(-1.23532).should eq(Bytes[191, 243, 195, 222, 231, 129, 131, 249])
    end
  end

  # describe "#write_i16" do
  #   it "writes int8" do
  #     serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new)
  #     serializer.serialize(12_i8).should eq(Bytes[0, 0, 0, 12])
  #   end
  # end

  # describe "#write_i16" do
  #   it "writes int8" do
  #     serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new)
  #     serializer.serialize(12_i8).should eq(Bytes[0, 0, 0, 12])
  #   end
  # end

  # describe "#write_i16" do
  #   it "writes int8" do
  #     serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new)
  #     serializer.serialize(12_i8).should eq(Bytes[0, 0, 0, 12])
  #   end
  # end

end
