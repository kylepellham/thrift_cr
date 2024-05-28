require "socket"
require "./base_transport.cr"

module Thrift
  class SocketTransport < BaseTransport

    def initialize(@host = "localhost", @port = 9090, @timeout : Int32? = nil)
      @desc = "#{@host}:#{@port}"
    end

    property handle : TCPSocket?
    property :timeout

    def open : TCPSocket
      last_exception = Exception.new("Could Not Resolve Address")
      ::Socket::Addrinfo.resolve(domain: @host, service: @port, type: ::Socket::Type::STREAM) do |addrinfo|
        begin
          host = addrinfo.ip_address.address
          port = addrinfo.ip_address.port
          socket = TCPSocket.new(host, port, connect_timeout: @timeout)
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

    def write(slice : Bytes) : Nil
      raise "closed stream" unless open?
      begin
        if handle = @handle
          sent = handle.send(slice)
          if sent < slice.size
            raise TransportException.new(TransportException::TIMED_OUT, "Socket: Timed out writing #{slice.size} bytes to #{@desc}")
          end
        else
          raise TransportException.new(TransportException::NOT_OPEN, "Transport is Nil")
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

    def read(slice : Bytes)
      raise "closed stream" unless open?
      read = 0
      begin
        if handle = @handle
          read, _ = handle.receive(slice)
        end
        if (read < 1)
          raise TransportException.new(TransportException::UNKNOWN, "Socket: Could not read #{slice.size} bytes from #{@desc}")
        end
      rescue ex : TransportException
        # don't let this get caught by the standard Exception handler
        raise ex
      rescue ex : Exception
        @handle.try(&.close)
        raise TransportException.new(TransportException::NOT_OPEN, ex.message)
      end
      read
    end

    def close
      @handle.try do |handle|
        handle.close unless handle.closed?
      end
      @handle = nil
    end

    def io : IO
      handle
    end

    def to_s
      "socket(#{@host}:#{@port})"
    end
  end
end
