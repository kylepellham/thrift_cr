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
require "./base_server_transport.cr"
require "./socket_transport.cr"

module Thrift
  module Transport
    # ServerSockerTransport wraps a TCPServer and incoming tcp connections are wrapped in a `Thrift::Transport::SocketTransport`
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
end
