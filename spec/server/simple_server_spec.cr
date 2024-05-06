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
      TCPSocket.open("localhost", port) do |sock|
      end
    end
    server.serve
  end
end