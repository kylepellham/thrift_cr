require "log"
require "./protocol/base_protocol.cr"
require "./types.cr"
require "./exceptions.cr"

module Thrift
  module Processor
    @logger : Log
    def initialize(handler, logger = nil)
      @handler = handler
      if logger.nil?
        @logger = ::Log.for(Processor, Log::Severity::Warn)
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
      def process(iprot : Thrift::BaseProtocol, oprot : Thrift::BaseProtocol)
        name, type, seqid = iprot.read_message_begin
        begin
            # begin
            #   # pp name, type, seqid
            #   call("#{name}", seqid, iprot, oprot)
            # rescue ex
            #   x = ApplicationException.new(ApplicationException::INTERNAL_ERROR, "Internal error")
            #   @logger.try(&.debug { "Internal error : #{ex.message}\n#{ex.backtrace.join("\n")}" })
            #   write_error(x, oprot, name, seqid)
            # end
            # true
          \{% begin %}
          case name
          \{% for method in @type.methods %}
            \{% if method.name.stringify[0.."process_".size - 1] == "process_" %}
          when "\{{method.name["process_".size..-1].id}}"
            \{{method.name}}(seqid, iprot, oprot)
            \{% end %}
          \{% end %}
          else
            iprot.skip(::Thrift::Types::Struct)
            iprot.read_message_end
            x = ::Thrift::ApplicationException.new(::Thrift::ApplicationException::UNKNOWN_METHOD, "Unknown function " + name)
            write_error(x, oprot, name, seqid)
            return false
          end
          \{% end %}
          return true
        rescue ex
          x = ::Thrift::ApplicationException.new(::Thrift::ApplicationException::INTERNAL_ERROR, "Internal error")
          @logger.debug { "Internal error : #{ex.message}\n#{ex.backtrace.join("\n")}" }
          write_error(x, oprot, name, seqid)
          return false
        end
      end

      def call(method : String, seqid : Int32, iprot : Thrift::BaseProtocol, oprot : Thrift::BaseProtocol)
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
