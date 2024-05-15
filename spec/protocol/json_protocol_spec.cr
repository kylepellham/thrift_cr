require "./spec_helper.cr"

describe ::Thrift::JsonProtocol do
  json_serializer = ::Thrift::Serializer.new ::Thrift::JsonProtocolFactory.new
  trans = ::Thrift::MemoryBufferTransport.new
  writer = ::Thrift::JsonProtocol.new(trans)

  before_each do
    trans.reset_buffer
  end

  describe "#write_message_begin" do
    it "writes correct message" do
      writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
      writer.write_message_end
      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq "[1,\"Test\",4,1"
    end
  end

  describe "#write_message_end" do
    it "writes message end" do
      writer.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
      writer.write_message_end
      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq ']'
    end
  end

  describe "#write_struct_begin" do
    it "writes struct begin" do
      writer.write_struct_begin "Test"
      writer.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[0].should eq '{'
    end
  end

  describe "#write_struct_end" do
    it "writes struct end" do
      writer.write_struct_begin "Test"
      writer.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq '}'
    end

    it "writes complete struct" do
      serial = json_serializer.serialize(TestClass.new("hello",12))
      String.new(serial).should eq %q({"1":{"i32":12},"2":{"str":"hello"}})
    end
  end

  describe "#write_field_begin" do
    it "writes type and id" do
      # need to write an entire struct
      writer.write_struct_begin "Test"
      writer.write_field_begin "TestField", ::Thrift::Types::I16, 1
      writer.write_i16 32_i16
      writer.write_field_end
      writer.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[1..11].should eq %q("1":{"i16":)
    end

    describe "#write_field_end" do
      it "writes closed field" do
        writer.write_struct_begin "Test"
        writer.write_field_begin "TestField", ::Thrift::Types::I16, 1
        writer.write_i16 32_i16
        writer.write_field_end
        writer.write_struct_end

        msg = trans.peek.not_nil!
        String.new(msg)[-2].should eq '}'
      end

      it "writes entire struct" do
        writer.write_struct_begin "Test"
        writer.write_field_begin "TestField", ::Thrift::Types::I16, 1
        writer.write_i16 32_i16
        writer.write_field_end
        writer.write_struct_end

        msg = trans.peek.not_nil!
        String.new(msg).should eq %q({"1":{"i16":32}})
      end
    end
  end

  describe "#write_map_begin" do
    it "writes map begin" do
      writer.write_map_begin ::Thrift::Types::Byte, ::Thrift::Types::String, 12
      writer.write_map_end

      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq %q(["i8","str",12,{})
    end
  end

  describe "#write_map_end" do
    it "writes map end" do
      writer.write_map_begin ::Thrift::Types::Byte, ::Thrift::Types::String, 12
      writer.write_map_end

      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq ']'
    end

    it "writes map" do
      serialized = json_serializer.serialize({"hello" => 14, "that" => 4124})
      String.new(serialized).should eq %q(["str","i32",2,{"hello":14,"that":4124}])
    end
  end

  describe "#write_list_begin" do
    it "writes type and size" do
      writer.write_list_begin ::Thrift::Types::I32, 12
      writer.write_list_end

      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq %q(["i32",12)
    end
  end

  describe "#write_list_end" do
    it "writes list end" do
      writer.write_list_begin ::Thrift::Types::I32, 12
      writer.write_list_end

      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq ']'
    end

    it "writes entire list" do
      serial = json_serializer.serialize([1, 2, 7, 12423, 2342])
      String.new(serial).should eq %q(["i32",5,1,2,7,12423,2342])
    end
  end

  # writing a set is the same as writing a list to don't test those

  describe "#write_bool" do
    it "writes true" do
      writer.write_bool true
      msg = trans.peek.not_nil!
      String.new(msg).should eq "1"
    end

    it "writes false" do
      writer.write_bool false
      msg = trans.peek.not_nil!
      String.new(msg).should eq "0"
    end
  end

  describe "#write_byte" do
    it "writes zero" do
      writer.write_byte 0
      msg = trans.peek.not_nil!
      String.new(msg).should eq "0"
    end

    it "writes greater than zero" do
      writer.write_byte 127_i8
      msg = trans.peek.not_nil!
      String.new(msg).should eq "127"
    end

    it "writes less than zero" do
      writer.write_byte -128_i8
      msg = trans.peek.not_nil!
      String.new(msg).should eq "-128"
    end
  end

  describe "#read_message_begin" do
    it "reads message begin" do
      trans.write(%q([1,"Test",4,1).to_slice)
      writer.read_message_begin.should eq({"Test", Thrift::MessageTypes::Oneway, 1})
    end
  end

  describe "#read_message_end" do
    it "reads end of message" do
      trans.write(%q(]).to_slice)
      writer.read_message_end
    end
  end

  describe "#read_field_begin" do
    it "reads beginning of field" do
    end
  end
end