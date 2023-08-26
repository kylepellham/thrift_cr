require "../byte_helpers.cr"

module Thrift
  class TransportException < Exception
    UNKNOWN = 0
    NOT_OPEN = 1
    ALREADY_OPEN = 2
    TIMED_OUT = 3
    END_OF_FILE = 4

    getter :type

    def initialize(@type=UNKNOWN, message=nil)
      super(message)
    end
  end

  class BaseTransport
    def open?; end
    
    def open; end

    def close; end

    # Reads a number of bytes from the transports. In Ruby 1.9+, the String returned will have a BINARY (aka ASCII8BIT) encoding.
    #
    # sz - The number of bytes to read from the transport.
    #
    # Returns a String acting as a byte buffer.
    def read(sz)
      raise NotImplementedError.new ""
    end

    # Returns an unsigned byte as a Fixnum in the range (0..255).
    def read_byte
      buf = read_all(1)
      return buf[0]
    end

    # Reads size bytes and copies them into buffer[0..size].
    def read_into_buffer(buffer, size)
      tmp = read_all(size)
      i = 0
      tmp.each do |byte|
        buffer[i] = byte
        i += 1
      end
      i
    end

    def read_all(size : Int32)
      return Bytes.new(0, 0) if size <= 0
      buf = read(size)
      while (buf.size < size)
        chunk = read(size - buf.size)
        buf << chunk
      end
    
      buf
    end

    # Writes the byte buffer to the transport. In Ruby 1.9+, the buffer will be forced into BINARY encoding.
    #
    # buf - A String acting as a byte buffer.
    #
    # Returns nothing.
    def write(buf); end
    def <<(buf); write(buf); end

    def flush; end

    def to_s
      "base"
    end
  end
  
  class BaseTransportFactory
    def get_transport(trans)
      return trans
    end
    
    def to_s
      "base"
    end
  end
end
