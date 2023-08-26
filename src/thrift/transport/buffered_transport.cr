require "./base_transport.cr"
require "../byte_helpers.cr"
module Thrift
  class BufferedTransport < BaseTransport
    DEFAULT_BUFFER = 4096

    @transport : BaseTransport
    @wbuf : Bytes
    @rbuf : Bytes
    @index : Int32 = 0

    def initialize(transport)
      @transport = transport
      @wbuf = Bytes.empty
      @rbuf = Bytes.empty
    end

    def open?
      return @transport.open?
    end

    def open
      @transport.open
    end

    def close
      flush
      @transport.close
    end

    def read(sz)
      @index += sz

      ret = if (tmp = @rbuf[(@index - sz)..(@index - 1)]).empty?
              Bytes.empty
            else
              tmp
            end

      if ret.empty?
        @rbuf = @transport.read(Math.max(sz, DEFAULT_BUFFER))
        @index = sz
        ret = if (tmp = @rbuf[0..sz]).empty?
                ret = Bytes.empty
              else
                tmp
              end
      end
      ret
    end

    def read_byte

      if @index >= @rbuf.size
        @transport.read(DEFAULT_BUFFER)
        @index = 0
      end
    
      @index += 1
      return @rbuf[@index - 1]
    end

    def read_into_buffer(buffer, size)
      i = 0
      while i < size
        if @index >= @rbuf.size
          @rbuf = @transport.read(DEFAULT_BUFFER)
          p! @rbuf
          @index = 0
        end

        byte = @rbuf[@index]
        buffer[i] = byte
        @index += 1
        i += 1
      end
      i
    end

    def write(buf)
      @wbuf = @wbuf.join buf
    end

    def flush
      unless @wbuf.empty?
        @transport.write(@wbuf)
        @wbuf = Bytes.empty
      end
      @transport.flush
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
