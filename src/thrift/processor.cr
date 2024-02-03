require "log"
require "./protocol/base_protocol.cr"
require "./types.cr"
require "./exceptions.cr"

module Thrift
  module Processor
    def initialize(handler, logger = nil)
      @handler = handler
      if logger.nil?
        @logger = Log.for(Processor, Log::Severity::Warn)
      else
        @logger = logger
      end
    end

    def read_args(iprot, args_class : ArgsClass.class) forall ArgsClass
      args = ArgsClass.read(iprot)
      iprot.read_message_end
      args
    end

    def write_result(result, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::Reply, seqid)
      result.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end

    def write_error(err, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::Exception, seqid)
      err.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end

    macro included
      def process(iprot : BaseProtocol, oprot : BaseProtocol)
        name, type, seqid = iprot.read_message_begin
        if \{{@type.id}}.methods.includes?("process_#{name}")
          begin
            # pp name, type, seqid
            send("#{name}", seqid, iprot, oprot)
          rescue ex
            x = ApplicationException.new(ApplicationException::INTERNAL_ERROR, "Internal error")
            @logger.try(&.debug { "Internal error : #{ex.message}\n#{ex.backtrace.join("\n")}" })
            write_error(x, oprot, name, seqid)
          end
          true
        else
          iprot.skip(Types::Struct)
          iprot.read_message_end
          x = ApplicationException.new(ApplicationException::UNKNOWN_METHOD, "Unknown function " + name)
          write_error(x, oprot, name, seqid)
          false
        end
      end

      def self.methods
        return \{{@type.methods.map &.name.stringify}}
      end

      def responds?(method_check)
        \{{@type.id}}.methods.includes?(method_check)
      end

      def send(method : String, seqid : Int32, iprot : Thrift::BaseProtocol, oprot : Thrift::BaseProtocol)
        \{% begin %}
        case method
        \{% for method in @type.methods %}
          \{% if method.name.stringify[0.."process_".size - 1] == "process_" %}
        when "\{{method.name.id}}"["process_".size..-1]
          \{{method.name}}(seqid, iprot, oprot)
          \{% end %}
        \{% end %}
        else
          raise ArgumentError.new "Method #{method} Not found"
        end
        \{% end %}
      end
    end
  end
end
