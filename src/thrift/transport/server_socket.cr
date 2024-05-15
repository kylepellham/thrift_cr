require "socket"
require "./base_server_transport.cr"
require "./socket.cr"

module Thrift
  class ServerSocketTransport < BaseServerTransport
    @handle : TCPServer?
    @host : String?
    @port : Int32

    def initialize(@host, @port)
    end

    def initialize(@port)
    end

    def listen
      if host = @host
        @handle = TCPServer.new(host, @port)
      else
        @handle = TCPServer.new(@port)
      end
    end

    def accept?
      @handle.try do |handle|
        sock = handle.accept
        trans = SocketTransport.new
        trans.handle = sock
        trans
      end
    end

    def close
      @handle.try do |handle|
        handle.close unless handle.closed?
      end
    end

    def closed?
      if handle = @handle
        handle.closed?
      else
        true
      end
    end

    def to_io
      @handle
    end

    def to_s
      "socket(#{@host}:#{@port})"
    end
  end
end
