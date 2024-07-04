require "./spec_helper.cr"

describe Thrift::ServerSocketTransport do
  it "initializes" do
    port = unused_local_port
    transport = Thrift::ServerSocketTransport.new("localhost", port)
  end

  it "listens" do
    port = unused_local_port
    transport = Thrift::ServerSocketTransport.new("localhost", port)
    transport.listen
    transport.handle.should_not be_a Nil
  end

  it "accepts" do
    port = unused_local_port
    transport = Thrift::ServerSocketTransport.new("localhost", port)
    transport.listen
    TCPSocket.open("localhost", port) do |sock|
      client = transport.accept
      client.should_not be_a Nil
    end
  end

  it "reads from client" do
    port = unused_local_port
    transport = Thrift::ServerSocketTransport.new("localhost", port)
    transport.listen
    TCPSocket.open("localhost", port) do |sock|
      client = transport.accept.not_nil!
      sock.write "hello".to_slice
      client.gets("hello".size).should eq "hello"
    end
  end

  it "writes to client" do
    port = unused_local_port
    transport = Thrift::ServerSocketTransport.new("localhost", port)
    transport.listen
    TCPSocket.open("localhost", port) do |sock|
      client = transport.accept.not_nil!
      client.write "hello".to_slice
      sock.gets("hello".size).should eq "hello"
    end
  end

end
