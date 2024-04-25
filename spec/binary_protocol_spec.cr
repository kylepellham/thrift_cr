require "./spec_helper.cr"

enum TestEnum
  TestValue1
  TestValue2 = 25
  TestValue3
end

class TestClass
  include ::Thrift::Struct
  @[::Thrift::Struct::Property(id: 1)]
  property inst_var1 : Int32?
  @[::Thrift::Struct::Property(id: 1)]
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


# helper to check list data
def check_list_data(data_size, buffer, *expected)
  # always this way for lists
  data_index = 5
  expected.each do |expected_res|
    buffer[data_index..data_index + data_size - 1].should eq(expected_res)
    data_index += data_size
  end
end

describe ::Thrift::BinaryProtocol do
  describe "Big Endian Encoded" do
    binary_serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new IO::ByteFormat::BigEndian)
    trans = ::Thrift::MemoryBufferTransport.new
    strict_writer = ::Thrift::BinaryProtocol.new(trans)
    non_strict_writer = ::Thrift::BinaryProtocol.new(trans, false, false)

    describe "#write_message_begin" do
      it "strict writes" do
        trans.clear
        strict_writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
        bytes = trans.read_all trans.available
        bytes[0..1].should eq(Bytes[128, 1]) # Bytes[128, 1] == 0x8001
        bytes[2..3].should eq(Bytes[0, 4]) # Bytes[0, 4] == 4 == MessageTypes::Oneway
        # size of string
        bytes[4..7].should eq(Bytes[0, 0, 0, 4]) # Bytes[0, 0, 0, 4] == "Test".size
        bytes[8..11].should eq(Bytes[84, 101, 115, 116]) # Bytes[84, 101, 115, 116] == "Test"
        bytes[12..15].should eq(Bytes[0, 0, 0, 1]) # Bytes[0, 0, 0, 1] == 1 == seqid
      end

      it "non-strict writes" do
        trans.clear
        non_strict_writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
        bytes = trans.read_all trans.available
        bytes[0..3].should eq(Bytes[0, 0, 0, 4])
        bytes[4..7].should eq(Bytes[84, 101, 115, 116]) # Bytes[84, 101, 115, 116] == "Test"
        bytes[8].should eq(4) # 4 == 4 == MessageType::Oneway
        bytes[9..12].should eq(Bytes[0, 0, 0, 1]) # Bytes[0, 0, 0, 1] == 1 == seqid
      end
    end


    describe "#write_byte" do
      it "writes Byte" do
        binary_serializer.serialize(12_u8).should eq(Bytes[12])
      end

      it "writes max Byte" do
        binary_serializer.serialize(UInt8::MAX).should eq(Bytes[255])
      end

      it "writes min Byte" do
        binary_serializer.serialize(UInt8::MIN).should eq(Bytes[0])
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

      it "writes enums" do
        binary_serializer.serialize(TestEnum::TestValue3).should eq(Bytes[0, 0, 0, 26])
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

    describe "#write_string" do
      it "writes String" do
        binary_serializer.serialize("Hello World").should eq(Bytes[0, 0, 0, 11, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100])
      end

      it "writes empty String" do
        binary_serializer.serialize("").should eq(Bytes[0, 0, 0, 0])
      end

      it "write non printable character" do
        binary_serializer.serialize("�").should eq(Bytes[0, 0, 0, 3, 239, 191, 189])
      end
    end

    describe "writing Containers" do
      describe "List" do
        type_index = 0
        size_index = 1

        it "writes empty List" do
          bytes = binary_serializer.serialize([] of UInt8) #.should eq(Bytes[3, 0, 0, 0, 0])
          bytes[type_index].should eq(::Thrift::Types::Byte.to_u8)
          bytes[size_index..(size_index + sizeof(Int32) - 1)].should eq(Bytes[0, 0, 0, 0])
        end

        it "writes List of Int8" do
          test_data = [1_u8, 5_u8, 255_u8]
          bytes = binary_serializer.serialize(test_data) #.should eq(Bytes[3, 0, 0, 0, 3, 1, 5, 255])
          size_end_index = size_index + sizeof(Int32) - 1
          bytes[type_index].should eq(::Thrift::Types::Byte.to_u8)
          bytes[size_index..size_end_index].should eq(Bytes[0, 0, 0, 3])
          check_list_data(sizeof(UInt8), bytes, Bytes[1], Bytes[5], Bytes[255])
        end

        it "writes List of Int16" do
          test_data = [1_i16, 5_i16, -1_i16]
          bytes = binary_serializer.serialize(test_data) #.should eq(Bytes[6, 0, 0, 0, 3, 0, 1, 0, 5, 255, 255])
          size_end_index = size_index + sizeof(Int32) - 1
          bytes[type_index].should eq(::Thrift::Types::I16.to_u8)
          bytes[size_index .. size_end_index].should eq(Bytes[0, 0, 0, 3])
          check_list_data(sizeof(Int16), bytes, Bytes[0, 1], Bytes[0, 5], Bytes[255, 255])
        end

        it "writes List of Int32" do
          test_data = [1, 5, -1]
          bytes = binary_serializer.serialize(test_data) #.should eq(Bytes[8, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 255, 255, 255, 255])
          size_end_index = size_index + sizeof(Int32) - 1
          bytes[type_index].should eq(::Thrift::Types::I32.to_u8)
          bytes[size_index .. size_end_index].should eq(Bytes[0, 0, 0, 3])
          check_list_data(sizeof(Int32), bytes, Bytes[0, 0, 0, 1], Bytes[0, 0, 0, 5], Bytes[255, 255, 255, 255])
        end

        it "writes List of Int64" do
          test_data = [1_i64, 5_i64, -1_i64]
          bytes = binary_serializer.serialize(test_data) #.should eq(Bytes[10, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 255, 255, 255, 255, 255, 255, 255, 255])
          size_end_index = size_index + sizeof(Int32) - 1
          bytes[type_index].should eq(::Thrift::Types::I64.to_u8)
          bytes[size_index .. size_end_index].should eq(Bytes[0, 0, 0, 3])
          check_list_data(sizeof(Int64), bytes, Bytes[0, 0, 0, 0, 0, 0, 0, 1], Bytes[0, 0, 0, 0, 0, 0, 0, 5], Bytes[255, 255, 255, 255, 255, 255, 255, 255])
        end

        it "writes List of Double" do
          binary_serializer.serialize([1_f64, 5_f64, -1_f64]).should eq(Bytes[4, 0, 0, 0, 3, 63, 240, 0, 0, 0, 0, 0, 0, 64, 20, 0, 0, 0, 0, 0, 0, 191, 240, 0, 0, 0, 0, 0, 0])
        end

        it "writes List of String" do
          binary_serializer.serialize(["Hello", "World", "!"]).should eq(Bytes[11, 0, 0, 0, 3, 0, 0, 0, 5, 72, 101, 108, 108, 111, 0, 0, 0, 5, 87, 111, 114, 108, 100, 0, 0, 0, 1, 33])
        end

        it "writes List of Enum" do
          binary_serializer.serialize([TestEnum::TestValue1, TestEnum::TestValue2, TestEnum::TestValue3]).should eq(Bytes[8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 26])
        end

        it "writes List of List" do
          binary_serializer.serialize([[1_i8, 2_i8], [3_i8, 4_i8, 5_i8]]).should eq(Bytes[15, 0, 0, 0, 2, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes List of Set" do
          binary_serializer.serialize([Set{1_i8, 2_i8}, Set{3_i8, 4_i8, 5_i8}]).should eq(Bytes[14, 0, 0, 0, 2, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes List of Map" do
          binary_serializer.serialize([{1 => "h"}, {2 => "e"}]).should eq(Bytes[13, 0, 0, 0, 2, 8, 11, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 104, 8, 11, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 101])
        end
      end

      describe "Set" do
        it "writes empty Set" do
          binary_serializer.serialize(Set(Int64).new).should eq(Bytes[10, 0, 0, 0, 0])
        end

        it "writes Set of Int8" do
          binary_serializer.serialize(Set{1_i8, 5_i8, -1_i8}).should eq(Bytes[3, 0, 0, 0, 3, 1, 5, 255])
        end

        it "writes Set of Int16" do
          binary_serializer.serialize(Set{1_i16, 5_i16, -1_i16}).should eq(Bytes[6, 0, 0, 0, 3, 0, 1, 0, 5, 255, 255])
        end

        it "writes Set of Int32" do
          binary_serializer.serialize(Set{1, 5, -1}).should eq(Bytes[8, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 255, 255, 255, 255])
        end

        it "writes Set of Int64" do
          binary_serializer.serialize(Set{1_i64, 5_i64, -1_i64}).should eq(Bytes[10, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 5, 255, 255, 255, 255, 255, 255, 255, 255])
        end

        it "writes Set of Double" do
          binary_serializer.serialize(Set{1_f64, 5_f64, -1_f64}).should eq(Bytes[4, 0, 0, 0, 3, 63, 240, 0, 0, 0, 0, 0, 0, 64, 20, 0, 0, 0, 0, 0, 0, 191, 240, 0, 0, 0, 0, 0, 0])
        end

        it "writes Set of String" do
          binary_serializer.serialize(Set{"Hello", "World", "!"}).should eq(Bytes[11, 0, 0, 0, 3, 0, 0, 0, 5, 72, 101, 108, 108, 111, 0, 0, 0, 5, 87, 111, 114, 108, 100, 0, 0, 0, 1, 33])
        end

        it "writes Set of Enum" do
          binary_serializer.serialize(Set{TestEnum::TestValue1, TestEnum::TestValue2, TestEnum::TestValue3}).should eq(Bytes[8, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 26])
        end

        it "writes Set of List" do
          binary_serializer.serialize(Set{[1_i8, 2_i8], [3_i8, 4_i8, 5_i8]}).should eq(Bytes[15, 0, 0, 0, 2, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes Set of Set" do
          binary_serializer.serialize(Set{Set{1_i8, 2_i8}, Set{3_i8, 4_i8, 5_i8}}).should eq(Bytes[14, 0, 0, 0, 2, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes Set of Map" do
          binary_serializer.serialize(Set(Hash(Int32, String)){ {1 => "h"}, {2 => "e"} }).should eq(Bytes[13, 0, 0, 0, 2, 8, 11, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 104, 8, 11, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 101])
        end
      end

      describe "Map" do
        it "writes empty Map" do
          binary_serializer.serialize({} of String => String).should eq(Bytes[11, 11, 0, 0, 0, 0])
        end

        it "writes empty Map" do
          binary_serializer.serialize([] of Int8).should eq(Bytes[3, 0, 0, 0, 0])
        end

        it "writes Map of Int8" do
          binary_serializer.serialize({1_i8 => 2_i8}).should eq(Bytes[3, 3, 0, 0, 0, 1, 1, 2])
        end

        it "writes Map of Int16" do
          binary_serializer.serialize({1_i16 => 2_i16}).should eq(Bytes[6, 6, 0, 0, 0, 1, 0, 1, 0, 2])
        end

        it "writes Map of Int32" do
          binary_serializer.serialize({1 => 2}).should eq(Bytes[8, 8, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2])
        end

        it "writes Map of Int64" do
          binary_serializer.serialize({1_i64 => 2_i64}).should eq(Bytes[10, 10, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2])
        end

        it "writes Map of Double" do
          binary_serializer.serialize({1_f64 => 2_f64}).should eq(Bytes[4, 4, 0, 0, 0, 1, 63, 240, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0])
        end

        it "writes Map of String" do
          binary_serializer.serialize({"Hello" => "World"}).should eq(Bytes[11, 11, 0, 0, 0, 1, 0, 0, 0, 5, 72, 101, 108, 108, 111, 0, 0, 0, 5, 87, 111, 114, 108, 100])
        end

        it "writes Map of Enum" do
          binary_serializer.serialize({TestEnum::TestValue1 => TestEnum::TestValue2}).should eq(Bytes[8, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25])
        end

        it "writes Map of List" do
          binary_serializer.serialize({[1_i8, 2_i8] => [3_i8, 4_i8, 5_i8]}).should eq(Bytes[15, 15, 0, 0, 0, 1, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes Map of Set" do
          binary_serializer.serialize({Set{1_i8, 2_i8} => Set{3_i8, 4_i8, 5_i8}}).should eq(Bytes[14, 14, 0, 0, 0, 1, 3, 0, 0, 0, 2, 1, 2, 3, 0, 0, 0, 3, 3, 4, 5])
        end

        it "writes Map of Map" do
          binary_serializer.serialize({ {1 => "h"} => {2 => "e"} }).should eq(Bytes[13, 13, 0, 0, 0, 1, 8, 11, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 104, 8, 11, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 101])
        end
      end
    end

    describe "writing struct" do
      it "writes populated field" do
        binary_serializer.serialize(TestClass.new(24, "")).should eq(Bytes[8, 0, 1, 0, 0, 0, 24, 11, 0, 1, 0, 0, 0, 0, 0])
      end

      it "writes non populated field" do
        binary_serializer.serialize(TestClass.new(nil, "")).should eq(Bytes[1, 0, 1, 11, 0, 1, 0, 0, 0, 0, 0])
      end

      it "throws with non populated required field" do
        expect_raises(Exception) do
          binary_serializer.serialize(TestClass.new(nil, nil))
        end
      end
    end

    describe "writing Union" do
      it "writes union" do
        union = UnionTest.new(map: {"hello" => 24})
        binary_serializer.serialize(union).should eq(Bytes[13, 0, 1, 11, 8, 0, 0, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 24, 0])
        union.string = "hello"
        binary_serializer.serialize(union).should eq Bytes[11, 0, 3, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0]
      end
    end
  end
end
