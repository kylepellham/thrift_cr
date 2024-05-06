require "./spec_helper.cr"

describe Thrift::Socket do
  it "initializes" do
    port = unused_local_port
    transport = Thrift::Socket.new("localhost", port)
    transport.to_s.should eq "socket(localhost:#{port})"
  end

  it "connects" do
    port = unused_local_port
    TCPServer.open("localhost", port) do |server|
      transport = Thrift::Socket.new("localhost", port)
      transport.open
      transport.open?.should be_true
    end
  end

  it "writes" do
    port = unused_local_port
    TCPServer.open("localhost", port) do |server|
      transport = Thrift::Socket.new("localhost", port)
      transport.open

      sock = server.accept

      transport.write("hello".to_slice)

      sock.gets("hello".size).should eq "hello"
    end
  end

  it "reads" do
    port = unused_local_port
    TCPServer.open("localhost", port) do |server|
      transport = Thrift::Socket.new("localhost", port)
      transport.open

      sock = server.accept
      sock << "hello"

      read_bytes = Bytes.new "hello".size
      transport.read(read_bytes)

      read_bytes.should eq "hello".to_slice
    end
  end

  it "closes" do
    port = unused_local_port
    TCPServer.open("localhost", port) do |server|
      transport = Thrift::Socket.new("localhost", port)
      transport.open
      transport.close
      transport.handle.should be_a Nil
    end
  end
end