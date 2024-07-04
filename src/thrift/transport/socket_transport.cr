#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

require "socket"
require "./base_transport.cr"
require "../thrift_logging.cr"

module Thrift
  module Transport
    # SocketTransport wraps a TCPSocket and adds thrift compatibility
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
            rescue ex : IO::TimeoutError | ::Socket::ConnectError
              Log.for(self.class).debug { ex }
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
end
