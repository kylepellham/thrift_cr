require "../transport/base_server_transport.cr"
require "../transport/base_transport.cr"
require "../protocol/base_protocol.cr"
require "../processor.cr"


module Thrift
  class BaseServer
    @processor : Thrift::Processor
    @server_transport : Thrift::BaseServerTransport
    @protocol_factory : Thrift::BaseProtocolFactory
    @transport_factory : Thrift::BaseTransportFactory
    def initialize(processor, server_transport, transport_factory=nil, protocol_factory=nil)
      @processor = processor
      @server_transport = server_transport
      @transport_factory = transport_factory ? transport_factory : Thrift::BaseTransportFactory.new
      @protocol_factory = protocol_factory ? protocol_factory : Thrift::BinaryProtocolFactory.new
    end

    def serve
      raise NotImplementedError.new ""
    end

    def to_s
      "server(#{@protocol_factory.to_s}(#{@transport_factory.to_s}(#{@server_transport.to_s})))"
    end
  end
end
