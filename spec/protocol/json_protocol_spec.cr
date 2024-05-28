require "./spec_helper.cr"

class ::Thrift::JsonProtocol
  def reset_context
    @contexts = [] of ::Thrift::JSONContext
    @context = ::Thrift::JSONContext.new
    @reader = ::Thrift::LookaheadReader.new @trans
  end
end

describe ::Thrift::JsonProtocol do
  json_serializer = ::Thrift::Serializer.new ::Thrift::JsonProtocolFactory.new
  trans = ::Thrift::MemoryBufferTransport.new
  prot = ::Thrift::JsonProtocol.new(trans)

  before_each do
    trans.reset_buffer
    prot.reset_context
  end

  describe "#write_message_begin" do
    it "writes correct message" do
      prot.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
      prot.write_message_end
      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq "[1,\"Test\",4,1"
    end
  end

  describe "#write_message_end" do
    it "writes message end" do
      prot.write_message_begin("Test", ::Thrift::MessageTypes::Oneway, 1)
      prot.write_message_end
      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq ']'
    end
  end

  describe "#write_struct_begin" do
    it "writes struct begin" do
      prot.write_struct_begin "Test"
      prot.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[0].should eq '{'
    end
  end

  describe "#write_struct_end" do
    it "writes struct end" do
      prot.write_struct_begin "Test"
      prot.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[-1].should eq '}'
    end

    it "writes complete struct" do
      serial = json_serializer.serialize(TestClass.new("hello", inst_var1: 12))
      String.new(serial).should eq %q({"2":{"str":"hello"},"1":{"i32":12}})
    end
  end

  describe "#write_field_begin" do
    it "writes type and id" do
      # need to write an entire struct
      prot.write_struct_begin "Test"
      prot.write_field_begin "TestField", ::Thrift::Types::I16, 1
      prot.write_i16 32_i16
      prot.write_field_end
      prot.write_struct_end

      msg = trans.peek.not_nil!
      String.new(msg)[1..11].should eq %q("1":{"i16":)
    end

    describe "#write_field_end" do
      it "writes closed field" do
        prot.write_struct_begin "Test"
        prot.write_field_begin "TestField", ::Thrift::Types::I16, 1
        prot.write_i16 32_i16
        prot.write_field_end
        prot.write_struct_end

        msg = trans.peek.not_nil!
        String.new(msg)[-2].should eq '}'
      end

      it "writes entire struct" do
        prot.write_struct_begin "Test"
        prot.write_field_begin "TestField", ::Thrift::Types::I16, 1
        prot.write_i16 32_i16
        prot.write_field_end
        prot.write_struct_end

        msg = trans.peek.not_nil!
        String.new(msg).should eq %q({"1":{"i16":32}})
      end
    end
  end

  describe "#write_map_begin" do
    it "writes map begin" do
      prot.write_map_begin ::Thrift::Types::Byte, ::Thrift::Types::String, 12
      prot.write_map_end

      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq %q(["i8","str",12,{})
    end
  end

  describe "#write_map_end" do
    it "writes map end" do
      prot.write_map_begin ::Thrift::Types::Byte, ::Thrift::Types::String, 12
      prot.write_map_end

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
      prot.write_list_begin ::Thrift::Types::I32, 12
      prot.write_list_end

      msg = trans.peek.not_nil!
      String.new(msg)[0..-2].should eq %q(["i32",12)
    end
  end

  describe "#write_list_end" do
    it "writes list end" do
      prot.write_list_begin ::Thrift::Types::I32, 12
      prot.write_list_end

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
      prot.write_bool true
      msg = trans.peek.not_nil!
      String.new(msg).should eq "1"
    end

    it "writes false" do
      prot.write_bool false
      msg = trans.peek.not_nil!
      String.new(msg).should eq "0"
    end
  end

  describe "#write_byte" do
    it "writes zero" do
      prot.write_byte 0
      msg = trans.peek.not_nil!
      String.new(msg).should eq "0"
    end

    it "writes greater than zero" do
      prot.write_byte 127_i8
      msg = trans.peek.not_nil!
      String.new(msg).should eq "127"
    end

    it "writes less than zero" do
      prot.write_byte -128_i8
      msg = trans.peek.not_nil!
      String.new(msg).should eq "-128"
    end
  end

  describe "#read_message_begin" do
    it "reads message begin" do
      trans.write(%q([1,"Test",4,1).to_slice)
      prot.read_message_begin.should eq({"Test", Thrift::MessageTypes::Oneway, 1})
    end
  end

  describe "#read_message_end" do
    it "reads end of message" do
      trans.write(%q(]).to_slice)
      prot.push_context(::Thrift::JSONListContext.new)
      prot.read_message_end
    end
  end

  describe "#read_field_begin" do
    it "reads beginning of field" do
    end
  end

  describe "#read_i32" do
    it "reads an int" do
      trans.write_string(123321.to_s.to_slice)
      prot.read_i32.should eq 123321
    end
  end
end