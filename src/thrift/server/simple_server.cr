require "./base_server.cr"
require "../transport/base_transport.cr"
require "../protocol/base_protocol.cr"

module Thrift
  # Crystal Event manager makes all network reads non blocking
  class SimpleServer < BaseServer
    def serve
      begin
        @server_transport.listen
        puts "1"
        loop do
          puts 2
          client = @server_transport.accept
          if client
            puts 3
            trans = @transport_factory.get_transport(client)
            prot = @protocol_factory.get_protocol(trans)
            begin
              loop do
                @processor.process(prot, prot)
              end
            rescue Thrift::TransportException | Thrift::ProtocolException
            ensure
              trans.close
            end
          else
            Fiber.yield
          end
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
