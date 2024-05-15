require "./spec_helper.cr"

describe Thrift::SimpleServer do
  it "initializes" do
    port = unused_local_port
    server_transport = Thrift::ServerSocketTransport.new("localhost", port)
    processor = TestProcessor.new TestHandler.new
    server = Thrift::SimpleServer.new(processor, server_transport)
  end

  it "serves" do
    port = unused_local_port
    server_transport = Thrift::ServerSocketTransport.new("localhost", port)
    processor = TestProcessor.new TestHandler.new
    server = Thrift::SimpleServer.new(processor, server_transport)
    spawn do
      server.serve
    end
    Fiber.yield

    sock = ::Thrift::SocketTransport.new("localhost", port)
    protocol = ::Thrift::BinaryProtocol.new(sock)
    sock.open

    protocol.write_message_begin("test", ::Thrift::MessageTypes::Call, 1)
    protocol.write_message_end
    Fiber.yield
    protocol.read_message_begin.should eq({"Test", ::Thrift::MessageTypes::Reply, 1})
    result = Test_result.read(from: protocol).should eq(Test_result.new result: "hello")
    protocol.read_message_end
  end
end