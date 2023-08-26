require "socket"
require "./base_transport.cr"

module Thrift
  class Socket < BaseTransport
    @handle : TCPSocket?
    def initialize(@host="localhost", @port=9090, @timeout : Int32? = nil)
      @desc = "#{@host}:#{@port}"
    end

    property :handle, :timeout

    def open : TCPSocket
      last_exception = Exception.new("Could Not Resolve Address")
      ::Socket::Addrinfo.resolve(domain: @host, service: @port, type: ::Socket::Type::STREAM) do |addrinfo|
        begin
          socket = TCPSocket.new(addrinfo.family)
          socket.tcp_nodelay = true
          begin
            socket.connect(addrinfo.ip_address)
          rescue IO::TimeoutError | ::Socket::ConnectError
            next
          end
          @handle = socket
          return socket
        rescue exception
          last_exception = exception
          next
        end
      end
      raise TransportException.new(TransportException::NOT_OPEN, "Could not connect to #{@desc}: #{last_exception.message}")
    end

    def open?
      !(handle = @handle).nil? && !handle.closed?
    end

    def write(msg)
      raise "closed stream" unless open?
      begin
        if @timeout.nil? || @timeout == 0
          if handle = @handle
            handle.send(msg)
            sent = msg.size
            if sent < msg.size
              raise TransportException.new(TransportException::TIMED_OUT, "Socket: Timed out writing #{msg.size} bytes to #{@desc}")
            end
          else
            raise TransportException.new(TransportException::NOT_OPEN, "Transport is Nil")
          end
        end
      rescue ex : TransportException
        # pass this on
        raise ex
      rescue ex
        @handle.try(&.close)
        @handle = nil
        raise TransportException.new(TransportException::NOT_OPEN, ex.message)
      end
    end

    def read(sz)
      raise "closed stream" unless open?
      buf = Bytes.new(sz, 0)
      read = 0
      begin
        if handle = @handle
          p! handle
          read, _ = handle.receive(buf)
          p! buf
        end
        if (read < 1)
          raise TransportException.new(TransportException::UNKNOWN, "Socket: Could not read #{sz} bytes from #{@desc}")
        end
      rescue ex : TransportException
        # don't let this get caught by the standard Exception handler
        raise ex
      rescue ex : Exception
        @handle.try(&.close)
        @handle = nil
        pp "failed"
        raise TransportException.new(TransportException::NOT_OPEN, ex.message)
      end
      buf
    end

    def close
      @handle.try do |handle|
        handle.close unless handle.closed?
      end
      @handle = nil
    end

    def to_io
      handle
    end

    def to_s
      "socket(#{@host}:#{@port})"
    end
  end
end