require "./spec_helper.cr"

describe "BaseProtocol" do

  describe Thrift::BaseProtocol do
    trans = Thrift::BaseTransport.new()
    prot = Thrift::BaseProtocol.new(trans)

    it "converts to string" do
      prot.to_s.should eq "base"
    end

    it "should make transport accessible" do
      prot.trans.should eq trans
    end
  end

  describe Thrift::BaseProtocolFactory do
    prot = Thrift::BaseProtocolFactory.new()
    it "converts to string" do
      prot.to_s.should eq "base"
    end
  end
end