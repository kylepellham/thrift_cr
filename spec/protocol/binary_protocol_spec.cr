require "./spec_helper.cr"


# helper to check list data
def check_list_data(data_size, buffer, *expected)
  # always this way for lists
  data_index = 5
  expected.each do |expected_res|
    buffer[data_index..data_index + data_size - 1].should eq(expected_res)
    data_index += data_size
  end
end

def manual_array_write(type : Args.class, trans, arr, *, byte_format = IO::ByteFormat::BigEndian) forall Args
  trans.write_bytes(Args.thrift_type.to_i8, byte_format)
  trans.write_bytes(arr.size, byte_format)
  arr.each do |ele|
    trans.write_bytes(ele, byte_format)
  end
end

describe ::Thrift::BinaryProtocol do
  it "initializes" do
    transport = Thrift::MemoryBufferTransport.new
    bprot = Thrift::BinaryProtocol.new(transport)
  end

  describe "Big Endian Encoded" do
    byte_format = IO::ByteFormat::BigEndian
    binary_serializer = ::Thrift::Serializer.new(::Thrift::BinaryProtocolFactory.new IO::ByteFormat::BigEndian)
    trans = ::Thrift::MemoryBufferTransport.new
    bprot = ::Thrift::BinaryProtocol.new(trans)

    before_each do
      trans.reset_buffer
    end

    describe "#write_message_begin" do
      strict_writer = ::Thrift::BinaryProtocol.new(trans)
      non_strict_writer = ::Thrift::BinaryProtocol.new(trans, false, false)

      it "strict writes" do

        strict_writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
        bytes = trans.peek.not_nil!
        bytes[0..1].should eq(Bytes[128, 1]) # Bytes[128, 1] == 0x8001
        bytes[2..3].should eq(Bytes[0, 4]) # Bytes[0, 4] == 4 == MessageTypes::Oneway
        # size of string
        bytes[4..7].should eq(Bytes[0, 0, 0, 4]) # Bytes[0, 0, 0, 4] == "Test".size
        bytes[8..11].should eq(Bytes[84, 101, 115, 116]) # Bytes[84, 101, 115, 116] == "Test"
        bytes[12..15].should eq(Bytes[0, 0, 0, 1]) # Bytes[0, 0, 0, 1] == 1 == seqid
      end

      it "non-strict writes" do

        non_strict_writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
        bytes = trans.peek.not_nil!
        bytes[0..3].should eq(Bytes[0, 0, 0, 4])
        bytes[4..7].should eq(Bytes[84, 101, 115, 116]) # Bytes[84, 101, 115, 116] == "Test"
        bytes[8].should eq(4) # 4 == 4 == MessageType::Oneway
        bytes[9..12].should eq(Bytes[0, 0, 0, 1]) # Bytes[0, 0, 0, 1] == 1 == seqid
      end
    end

    describe "#write_field_begin" do
      it "writes field correctly" do

        bprot.write_field_begin("Something", Int32.thrift_type, 0_i16)
        trans.peek.not_nil!.should eq Bytes[8, 0, 0]
      end
    end

    describe "#write_map_begin" do
      it "writes map correclty" do

        bprot.write_map_begin(String.thrift_type, Int32.thrift_type, 12)
        trans.peek.not_nil!.should eq Bytes[String.thrift_type.to_i8, Int32.thrift_type.to_i8, 0, 0, 0, 12]
      end
    end

    describe "#write_list_begin" do
      it "writes list correctly" do

        bprot.write_list_begin(Float64.thrift_type, 12)
        trans.peek.not_nil!.should eq Bytes[Float64.thrift_type.to_i8, 0, 0, 0, 12]
      end
    end

    describe "#write_set_begin" do
      it "writes set correctly" do

        bprot.write_set_begin(Float64.thrift_type, 12)
        trans.peek.not_nil!.should eq Bytes[Float64.thrift_type.to_i8, 0, 0, 0, 12]
      end
    end

    describe "#write_field_stop" do
      it "writes fields correctly" do

        bprot.write_field_stop
        trans.peek.not_nil!.should eq Bytes[Thrift::Types::Stop.to_i8]
      end
    end

    describe "#write_bool" do
      it "writes true" do
        binary_serializer.serialize(true).should eq Bytes[1]
      end

      it "writes false" do
        binary_serializer.serialize(false).should eq Bytes[0]
      end
    end

    describe "#write_byte" do
      it "writes Byte" do
        binary_serializer.serialize(12_i8).should eq(Bytes[12])
      end

      it "writes max Byte" do
        binary_serializer.serialize(Int8::MAX).should eq(Bytes[127])
      end

      it "writes min Byte" do
        binary_serializer.serialize(Int8::MIN).should eq(Bytes[128])
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

    describe "#write_uuid" do
      it "writes empty uuid" do
        binary_serializer.serialize(UUID.empty).should eq UUID.empty.bytes.to_slice
      end
    end

    describe "#write_binary" do
      it "write empty bytes" do
        binary_serializer.serialize(Bytes[]).should eq Bytes[0, 0, 0, 0]
      end

      it "writes several bytes" do
        binary_serializer.serialize(Bytes[1, 2, 3]).should eq Bytes[0, 0, 0, 3, 1, 2, 3]
      end
    end

    describe "writing Containers" do
      describe "Array(T)#write" do
        type_index = 0
        size_index = 1

        it "writes empty List" do
          bytes = binary_serializer.serialize([] of Int8) #.should eq(Bytes[3, 0, 0, 0, 0])
          bytes[type_index].should eq(::Thrift::Types::Byte.to_i8)
          bytes[size_index..(size_index + sizeof(Int32) - 1)].should eq(Bytes[0, 0, 0, 0])
        end

        it "writes List of Int8" do
          test_data = [1_i8, 5_i8, -128_i8]
          bytes = binary_serializer.serialize(test_data) #.should eq(Bytes[3, 0, 0, 0, 3, 1, 5, 255])
          size_end_index = size_index + sizeof(Int32) - 1
          bytes[type_index].should eq(::Thrift::Types::Byte.to_i8)
          bytes[size_index..size_end_index].should eq(Bytes[0, 0, 0, 3])
          check_list_data(sizeof(UInt8), bytes, Bytes[1], Bytes[5], Bytes[128])
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

      describe "Set(T)#write" do
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

      describe "Hash(K, V)#write" do
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

    describe "Thrift::Struct#write" do
      it "writes populated field" do
        binary_serializer.serialize(TestClass.new("", inst_var1: 24)).should eq(Bytes[8, 0, 1, 0, 0, 0, 24, 8, 0, 2, 0, 0, 0, 0, 0])
      end

      it "writes non populated field" do
        binary_serializer.serialize(TestClass.new("", inst_var1: nil)).should eq(Bytes[8, 0, 2, 0, 0, 0, 0, 0])
      end
    end

    describe "Thrift::Union#write" do
      it "writes union" do
        union = UnionTest.new(map: {"hello" => 24})
        binary_serializer.serialize(union).should eq(Bytes[13, 0, 1, 11, 8, 0, 0, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0, 0, 0, 24, 0])
        union.string = "hello"
        binary_serializer.serialize(union).should eq Bytes[11, 0, 3, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0]
      end
    end

    describe "#read_message_begin" do
      it "reads message correctly" do

        message = Bytes[128, 1, 0, 4, 0, 0, 0, 4, 84, 101, 115, 116, 0, 0, 0, 1]
        trans.write(message)
        bprot.read_message_begin.should eq({"Test", Thrift::MessageTypes::Oneway, 1})
      end
    end

    describe "#read_field_begin" do
      it "read any field" do

        message = Bytes[Int32.thrift_type.to_i8, 0, 1]
        trans.write(message)
        bprot.read_field_begin.should eq({"", Int32.thrift_type, 1})
      end

      it "reads field stop" do

        message = Bytes[Thrift::Types::Stop.to_i8, 255, 1]
        trans.write(message)
        bprot.read_field_begin.should eq({"", Thrift::Types::Stop, 0})
      end
    end

    describe "#read_map_begin" do
      it "reads map correctly" do

        message = Bytes[String.thrift_type.to_i8, Int32.thrift_type.to_i8, 0, 0, 0, 12]
        trans.write(message)
        bprot.read_map_begin.should eq({String.thrift_type, Int32.thrift_type, 12})
      end
    end

    describe "#read_list_begin" do
      it "reads list correctly" do

        message = Bytes[Float64.thrift_type.to_i8, 0, 0, 0, 255]
        trans.write(message)
        bprot.read_set_begin.should eq({Float64.thrift_type, 255})
      end
    end

    describe "#read_set_begin" do
      it "reads set correctly" do

        message = Bytes[Float64.thrift_type.to_i8, 0, 0, 0, 255]
        trans.write(message)
        bprot.read_set_begin.should eq({Float64.thrift_type, 255})
      end
    end

    describe "#read_bool" do
      it "reads true" do

        trans.write_bytes(1_i8, byte_format)
        bprot.read_bool.should eq true
      end

      it "reads false" do

        trans.write_bytes(0_i8)
        bprot.read_bool.should eq false
      end
    end

    describe "#read_byte" do
      it "reads 0" do
        trans.write_bytes(0_i8, byte_format)
        bprot.read_byte.should eq 0_i8
      end

      it "read Int8::max" do
        trans.write_bytes(Int8::MAX, byte_format)
        bprot.read_byte.should eq Int8::MAX
      end

      it "reads -1" do
        trans.write_bytes(-1, byte_format)
        bprot.read_byte.should eq -1
      end
    end

    describe "#read_i16" do
      it "reads 0" do
        trans.write_bytes(0_i16, byte_format)
        bprot.read_i16.should eq 0_i8
      end

      it "reads Int16::MAX" do
        trans.write_bytes(Int16::MAX, byte_format)
        bprot.read_i16.should eq Int16::MAX
      end

      it "reads -1" do
        trans.write_bytes(-1_i16, byte_format)
        bprot.read_i16.should eq -1_i16
      end
    end

    describe "#read_i32" do
      it "reads 0" do
        trans.write_bytes(0, byte_format)
        bprot.read_i32.should eq 0
      end

      it "reads Int32::MAX" do
        trans.write_bytes(Int32::MAX, byte_format)
        bprot.read_i32.should eq Int32::MAX
      end

      it "reads -1" do
        trans.write_bytes(-1, byte_format)
        bprot.read_i32.should eq -1
      end
    end

    describe "#read_i64" do
      it "reads 0" do
        trans.write_bytes(0_i64, byte_format)
        bprot.read_i64.should eq 0
      end

      it "reads Int64::MAX" do
        trans.write_bytes(Int64::MAX, byte_format)
        bprot.read_i64.should eq Int64::MAX
      end

      it "reads -1" do
        trans.write_bytes(-1_i64, byte_format)
        bprot.read_i64.should eq -1_i64
      end
    end

    describe "#read_double" do
      it "reads 0" do
        trans.write_bytes(0_f64, byte_format)
        bprot.read_i64.should eq 0
      end

      it "reads Float64::MAX" do
        trans.write_bytes(Float64::MAX, byte_format)
        bprot.read_double.should eq Float64::MAX
      end

      it "read Float64::MIN" do
        trans.write_bytes(Float64::MIN, byte_format)
        bprot.read_double.should eq Float64::MIN
      end

      it "reads -1.2344554" do
        trans.write_bytes(-1.2344554_f64, byte_format)
        bprot.read_double.should eq -1.2344554_f64
      end
    end

    describe "#read_uuid" do
      it "reads empty uuid" do
        trans.write(UUID.empty.bytes.to_slice)
        bprot.read_uuid.should eq UUID.empty
      end
    end

    describe "#read_string" do
      it "reads empty string" do
        str = ""
        trans.write_bytes(str.size, byte_format)
        trans.write(str.to_slice)
        bprot.read_string.should eq ""
      end

      it "reads utf-8 string" do
        str = "hello"
        trans.write_bytes(str.size, byte_format)
        trans.write(str.to_slice)
        bprot.read_string.should eq "hello"
      end
    end

    describe "#read_binary" do
      it "writes empty binary" do
        message = Bytes.empty
        trans.write_bytes(message.size, byte_format)
        trans.write(message)
        bprot.read_binary.should eq message
      end

      it "writes arbitrary data" do
        message = Bytes[1, 3423, 52312, 2315421]
        trans.write_bytes(message.size, byte_format)
        trans.write(message)
        bprot.read_binary.should eq message
      end
    end

    describe "Array(T)#read" do
      it "reads empty list" do
        manual_array_write(Int32, trans, [] of Int8, byte_format: byte_format)
        Array(Int32).read(from: bprot).should eq [] of Int32
      end

      it "reads list of Bytes" do
        arr = [0_i8, 12_i8, 127_i8]
        manual_array_write(Int8, trans, arr, byte_format: byte_format)
        Array(Int8).read(bprot).should eq arr
      end

      it "reads list of Int16" do
        arr = [0_i16, 12_i16, 127_i16]
        manual_array_write(Int16, trans, arr, byte_format: byte_format)
        Array(Int16).read(bprot).should eq arr
      end

      it "reads list of Int32" do
        arr = [0, 12, 127]
        manual_array_write(Int32, trans, arr, byte_format: byte_format)
        Array(Int32).read(bprot).should eq arr
      end

      it "reads list of int64" do
        arr = [0_i64, 12_i64, 127_i64]
        manual_array_write(Int64, trans, arr, byte_format: byte_format)
        Array(Int64).read(bprot).should eq arr
      end

      it "reads list of Float64" do
        arr = [0_f64, -123.3682, Float64::MAX]
        manual_array_write(Float64, trans, arr, byte_format: byte_format)
        Array(Float64).read(bprot).should eq arr
      end

      it "reads list of UUID" do
        arr = [UUID.empty, UUID.empty]
        trans.write_bytes(UUID.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write(ele.bytes.to_slice)
        end
        Array(UUID).read(bprot).should eq arr
      end

      it "reads list of String" do
        arr = ["Hello", "World", "extra"]
        trans.write_bytes(String.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(ele.size, byte_format)
          trans.write(ele.to_slice)
        end
        Array(String).read(bprot).should eq arr
      end

      it "reads list of Bytes" do
        arr = ["Hello".to_slice, "World".to_slice, "extra".to_slice]
        trans.write_bytes(Bytes.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(ele.size, byte_format)
          trans.write(ele)
        end
        Array(Bytes).read(bprot).should eq arr
      end

      it "reads list of lists" do
        arr = [[1, 2], [3, 4, 5], [6]]
        trans.write_bytes(Array.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(ele.size, byte_format)
          ele.each do |ele2|
            trans.write_bytes(ele2, byte_format)
          end
        end
        Array(Array(Int32)).read(bprot).should eq arr
      end

      it "reads list of set" do
        arr = [Set(Int32){1, 2}, Set(Int32){3, 4, 5}, Set(Int32){6}]
        trans.write_bytes(Set.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(ele.size, byte_format)
          ele.each do |ele2|
            trans.write_bytes(ele2, byte_format)
          end
        end
        Array(Set(Int32)).read(bprot).should eq arr
      end

      it "reads list of Hash" do
        arr = [{"hello" => 12}, {"there" => 89989, "world" => -1}]
        trans.write_bytes(Hash.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |arr_ele|
          trans.write_bytes(String.thrift_type.to_i8, byte_format)
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(arr_ele.size, byte_format)
          arr_ele.each do |k ,v|
            trans.write_bytes(k.size, byte_format)
            trans.write(k.to_slice)
            trans.write_bytes(v, byte_format)
          end
        end
        Array(Hash(String, Int32)).read(bprot).should eq arr
      end
    end

    describe "Set(T)#read" do
      it "reads empty set" do
        arr = Set(Int32).new
        manual_array_write(Int32, trans, arr, byte_format: byte_format)
        Set(Int32).read(from: bprot).should eq arr
      end

      it "reads set of Bytes" do
        arr = [0_i8, 12_i8, 127_i8].to_set
        manual_array_write(Int8, trans, arr, byte_format: byte_format)
        Set(Int8).read(bprot).should eq arr
      end

      it "reads set of Int16" do
        arr = [0_i16, 12_i16, 127_i16].to_set
        manual_array_write(Int16, trans, arr, byte_format: byte_format)
        Set(Int16).read(bprot).should eq arr
      end

      it "reads set of Int32" do
        arr = [0, 12, 127].to_set
        manual_array_write(Int32, trans, arr, byte_format: byte_format)
        Set(Int32).read(bprot).should eq arr
      end

      it "reads set of int64" do
        arr = [0_i64, 12_i64, 127_i64].to_set
        manual_array_write(Int64, trans, arr, byte_format: byte_format)
        Set(Int64).read(bprot).should eq arr
      end

      it "reads set of Float64" do
        arr = [0_f64, -123.3682, Float64::MAX].to_set
        manual_array_write(Float64, trans, arr, byte_format: byte_format)
        Set(Float64).read(bprot).should eq arr
      end

      it "reads set of UUID" do
        arr = [UUID.empty, UUID.empty].to_set
        trans.write_bytes(UUID.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write(ele.bytes.to_slice)
        end
        Set(UUID).read(bprot).should eq [UUID.empty].to_set
      end

      it "reads set of String" do
        arr = ["Hello", "World", "extra"].to_set
        trans.write_bytes(String.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(ele.size, byte_format)
          trans.write(ele.to_slice)
        end
        Set(String).read(bprot).should eq arr
      end

      it "reads set of Bytes" do
        arr = ["Hello".to_slice, "World".to_slice, "extra".to_slice].to_set
        trans.write_bytes(Bytes.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(ele.size, byte_format)
          trans.write(ele)
        end
        Set(Bytes).read(bprot).should eq arr
      end

      it "reads set of lists" do
        arr = [[1, 2], [3, 4, 5], [6]].to_set
        trans.write_bytes(Array.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(ele.size, byte_format)
          ele.each do |ele2|
            trans.write_bytes(ele2, byte_format)
          end
        end
        Set(Array(Int32)).read(bprot).should eq arr
      end

      it "reads set of set" do
        arr = [Set(Int32){1, 2}, Set(Int32){3, 4, 5}, Set(Int32){6}].to_set
        trans.write_bytes(Set.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |ele|
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(ele.size, byte_format)
          ele.each do |ele2|
            trans.write_bytes(ele2, byte_format)
          end
        end
        Set(Set(Int32)).read(bprot).should eq arr
      end

      it "reads set of Hash" do
        arr = [{"hello" => 12}, {"there" => 89989, "world" => -1}].to_set
        trans.write_bytes(Hash.thrift_type.to_i8, byte_format)
        trans.write_bytes(arr.size, byte_format)
        arr.each do |arr_ele|
          trans.write_bytes(String.thrift_type.to_i8, byte_format)
          trans.write_bytes(Int32.thrift_type.to_i8, byte_format)
          trans.write_bytes(arr_ele.size, byte_format)
          arr_ele.each do |k ,v|
            trans.write_bytes(k.size, byte_format)
            trans.write(k.to_slice)
            trans.write_bytes(v, byte_format)
          end
        end
        Set(Hash(String, Int32)).read(bprot).should eq arr
      end

    end
  end
end