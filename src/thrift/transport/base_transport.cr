require "../helpers.cr"

module Thrift
  class TransportException < Exception
    UNKNOWN      = 0
    NOT_OPEN     = 1
    ALREADY_OPEN = 2
    TIMED_OUT    = 3
    END_OF_FILE  = 4

    getter :type

    def initialize(@type = UNKNOWN, message = nil)
      super(message)
    end
  end

  # BaseTransport Inherits from IO for some nice standard library tools later
  abstract class BaseTransport < IO
    abstract def open?
    def open; end

    def close; end

    def to_io
      nil
    end

    def read_byte
      bytes = Bytes.new(1, 0)
      bytes_read = read(bytes)
      return bytes_read ? bytes[0] : nil
    end

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
