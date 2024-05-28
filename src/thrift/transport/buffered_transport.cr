require "./base_transport.cr"
require "../helpers.cr"

module Thrift
  class BufferedTransport < BaseTransport
    include IO::Buffered

    @transport : BaseTransport
    def initialize(@transport)
    end

    def unbuffered_read(slice : Bytes)
      @transport.read(slice)
    end

    def unbuffered_write(slice : Bytes)
      @transport.write(slice)
    end

    def unbuffered_flush
      @transport.flush
    end

    def unbuffered_close
      @transport.close
    end

    def unbuffered_rewind
      @pos = 0
    end

    def open?
      return @transport.open?
    end

    def to_s
      "buffered(#{@transport.to_s})"
    end
  end

  class BufferedTransportFactory < BaseTransportFactory
    def get_transport(transport)
      return BufferedTransport.new(transport)
    end

    def to_s
      "buffered"
    end
  end
end
