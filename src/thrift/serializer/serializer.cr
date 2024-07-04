require "../protocol/binary_protocol.cr"
require "../transport/memory_buffer_transport.cr"

module Thrift
  # Serializer will simply serializes thrift data using the provided Protocol Factory
  class Serializer
    @protocol : Protocol::BaseProtocol

    def initialize(protocol_factory = Protocol::BinaryProtocolFactory.new)
      @transport = Transport::MemoryBufferTransport.new
      @protocol = protocol_factory.get_protocol(@transport)
    end

    def serialize(base)
      @transport.reset_buffer
      base.write(@protocol)
      buf = Bytes.new(@transport.available)
      @transport.read(buf)
      buf
    end
  end
end
