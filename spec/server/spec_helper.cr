require "spec"
require "../../src/thrift.cr"

def unused_local_port
  TCPServer.open("::", 0) do |server|
    server.local_address.port
  end
end

class Test_result
  include ::Thrift::Struct

  @[::Thrift::Struct::Property(fid: 1, requirement: :optional)]
  struct_property result : String?
  @[::Thrift::Struct::Property(fid: 2, requirement: :optional)]
  struct_property success : Bool?

  def initialize(*, result = nil, success = nil)
    result.try do |result|
      @result = result
      @__isset.result = true
    end

    success.try do |success|
      @success = success
      @__isset.success = true
    end
  end
end


class TestHandler
  def test
    Fiber.yield
    "hello"
  end
end

class TestProcessor
  include Thrift::Processor
  @handler : TestHandler

  def process_test(seqid, oprot, iprot)
    result = Test_result.new
    result.result = @handler.test
    write_result(result, oprot, "Test", seqid)
  end
end
