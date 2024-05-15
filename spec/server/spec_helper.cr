require "spec"
require "../../src/thrift.cr"

def unused_local_port
  TCPServer.open("::", 0) do |server|
    server.local_address.port
  end
end

class Test_result
  include ::Thrift::Struct

  setter result : String?
  setter success : Bool?

  def initialize(@result = nil, @success = nil)
  end

  def write(to oprot : ::Thrift::BaseProtocol)
    oprot.write_struct_begin("Test_result")
    oprot.write_recursion do
      @result.try do |result|
        oprot.write_field_begin("result", String.thrift_type, 0_i16)
        result.write to: oprot
        oprot.write_field_end
      end

      @success.try do |success|
        oprot.write_field_begin("success", Bool.thrift_type, 1_i16)
        success.write to: oprot
        oprot.write_field_end
      end

      oprot.write_field_stop
      oprot.write_struct_end
    end

  end

  def read(from iprot : ::Thrift::BaseProtocol)
    iprot.read_recursion do
      iprot.read_struct_begin
      loop do
        name, ftype, fid = iprot.read_field_begin
        break if ftype == ::Thrift::Types::Stop
        case {fid, ftype}
        when {0, String.thrift_type}
          @result = String.read from: iprot
        when {1, Bool.thrift_type}
          @success = Bool.read from: iprot
        else
          iprot.skip(ftype)
        end
        iprot.read_field_end
      end
      iprot.read_struct_end
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
