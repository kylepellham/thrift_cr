require "./spec_helper.cr"
require "../../src/thrift.cr"


describe Thrift::MemoryBufferTransport do
  it "initializes" do
    buffer = Thrift::MemoryBufferTransport.new
    buffer.to_s.should eq "memory"
  end

  it "writes" do
    buffer = Thrift::MemoryBufferTransport.new
    buffer.write("hello world".to_slice)
    buffer.available.should eq "hello world".size
    bytes = Bytes.new "hello world".size
    buffer.read(bytes)
    bytes.should eq "hello world".to_slice
  end

  it "reads byte" do
    buffer = Thrift::MemoryBufferTransport.new
    buffer.write("hello".to_slice)
    buffer.read_byte.should eq 'h'.ord
    buffer.read_byte.should eq 'e'.ord
    buffer.read_byte.should eq 'l'.ord
    buffer.read_byte.should eq 'l'.ord
    buffer.read_byte.should eq 'o'.ord
  end

  it "reads" do
    buffer = Thrift::MemoryBufferTransport.new
    buffer.write("a".to_slice)
    bytes = Bytes.new(1)
    buffer.read bytes
    bytes.should eq Bytes['a'.ord]
    buffer.write("hello".to_slice)
    bytes = Bytes.new("hello".size)
    buffer.read(bytes)
    bytes.should eq "hello".to_slice
  end

  it "throws on writing too much data" do
    buffer = Thrift::MemoryBufferTransport.new(8)
    expect_raises(IO::Error, "Overflow") do
      buffer.write("123456789".to_slice)
    end
  end
end