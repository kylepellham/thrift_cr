require "spec"
require "../../src/thrift.cr"

def unused_local_port
  TCPServer.open("::", 0) do |server|
    server.local_address.port
  end
end

class TestHandler
  def test
    return 12
  end
end

class TestProcessor
  include Thrift::Processor
  @handler : TestHandler

  def process_test(oprot, iprot)
    oprot.write_string "success"
  end
end
