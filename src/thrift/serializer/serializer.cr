require "../protocol/binary_protocol.cr"
require "../transport/memory_buffer_transport.cr"

module Thrift
  class Serializer
    @protocol : BaseProtocol

    def initialize(protocol_factory = BinaryProtocolFactory.new)
      @transport = MemoryBufferTransport.new
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
