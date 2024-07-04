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

require "./base_server.cr"
require "../transport/base_transport.cr"
require "../protocol/base_protocol.cr"

module Thrift
  module Server
    # SimpleServer is a server that only allows a single connection at a time.
    # SimpleServer is fiber-safe
    class SimpleServer < BaseServer
      def serve
        interrupt = false
        begin
          @server_transport.listen
          until interrupt
            @server_transport.accept do |client|
              trans = @transport_factory.get_transport(client)
              prot = @protocol_factory.get_protocol(trans)
              begin
                loop do
                  select
                  when int_set = interrupt_ch.receive?
                    interrupt = int_set
                    break
                  else
                    @processor.process(prot, prot)
                  end
                end
                Fiber.yield
              rescue Thrift::Transport::TransportException | Thrift::Protocol::ProtocolException
              ensure
                trans.close
              end
            end
            Fiber.yield
          end
        ensure
          @server_transport.close
        end
      end

      def to_s
        "simple(#{super.to_s})"
      end
    end
  end
end