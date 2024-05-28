require "./base_server.cr"
require "../transport/base_transport.cr"
require "../protocol/base_protocol.cr"

module Thrift
  # Crystal Event manager makes all network reads non blocking
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
            rescue Thrift::TransportException | Thrift::ProtocolException
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
