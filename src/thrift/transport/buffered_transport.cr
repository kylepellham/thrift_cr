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
    # DEFAULT_BUFFER = 4096

    # @transport : BaseTransport
    # @wbuf : Bytes
    # @rbuf : Bytes
    # @index : Int32 = 0

    # def initialize(transport)
    #   @transport = transport
    #   @wbuf = Bytes.empty
    #   @rbuf = Bytes.empty
    # end

    # def open?
    #   return @transport.open?
    # end

    # def open
    #   @transport.open
    # end

    # def close
    #   flush
    #   @transport.close
    # end

    # def read(slice : Bytes) : Int32
    #   @index += slice.size

    #   ret = if (tmp = @rbuf[(@index - slice.size)..(@index - 1)]).empty?
    #           Bytes.empty
    #         else
    #           tmp
    #         end

    #   if ret.empty?
    #     @rbuf = @transport.read_all(Math.max(slice.size, DEFAULT_BUFFER))
    #     @index = slice.size
    #     ret = if (tmp = @rbuf[0..slice.size - 1]).empty?
    #             Bytes.empty
    #           else
    #             tmp
    #           end
    #   end
    #   slice.copy_from(ret)
    #   slice.size
    # end

    # def read_byte
    #   if @index >= @rbuf.size
    #     @transport.read_all(DEFAULT_BUFFER)
    #     @index = 0
    #   end

    #   @index += 1
    #   return @rbuf[@index - 1]
    # end

    # def write(buf) : Nil
    #   @wbuf = @wbuf.join_with buf
    # end

    # def flush
    #   unless @wbuf.empty?
    #     @transport.write(@wbuf)
    #     @wbuf = Bytes.empty
    #   end
    #   @transport.flush
    # end

    # def io : IO
    #   @transport.io
    # end

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
