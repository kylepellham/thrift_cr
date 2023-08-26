require "log"
require "./protocol/base_protocol.cr"
require "./types.cr"
require "./exceptions.cr"

module Thrift
  module Processor

    @handler : Nil
    def initialize(handler, logger = nil)
      @handler = handler
      if logger.nil?
        @logger = Log.for(Processor, Log::Severity::Warn)
      else
        @logger = logger
      end
    end

    def responds?(method_check)
      false
    end

    def process(iprot : BaseProtocol, oprot : BaseProtocol)
      name, type, seqid = iprot.read_message_begin
      if {{@type.id}}.methods.includes?("process_#{name}")
        begin
          pp name, type, seqid
          send("process_#{name}", seqid, iprot, oprot)
        rescue ex
          x = ApplicationException.new(ApplicationException::INTERNAL_ERROR, "Internal error")
          @logger.try(&.debug {"Internal error : #{ex.message}\n#{ex.backtrace.join("\n")}"})
          write_error(x, oprot, name, seqid)
        end
        true
      else
        iprot.skip(Types::STRUCT)
        iprot.read_message_end
        x = ApplicationException.new(ApplicationException::UNKNOWN_METHOD, "Unknown function "+name)
        write_error(x, oprot, name, seqid)
        false
      end
    end

    def read_args(iprot, args_class)
      args = args_class.new
      args.read(iprot)
      iprot.read_message_end
      args
    end

    def write_result(result, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::REPLY, seqid)
      result.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end

    def write_error(err, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::EXCEPTION, seqid)
      err.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end

    macro included
      {% verbatim do %}
        macro finished

          def self.methods(method_name)
            return {{@type.methods.select(&.stringify.startswith("process_")).map &.name.stringify}}
          end

          def send(method : String, seqid : Int32, iprot : Thrift::BaseProtocol, oprot : Thrift::BaseProtocol)
            case method
            {% for method in @type.methods %}
              {% if method.name.stringify[0.."process_".size] == "process_" %}
            when "{{method.name.id}}"
              @handler.{{method.name.stringify.l_strip("process_").id}}(seqid, iprot, oprot)
              {% end %}
            {% end %}
            else
              raise ArgumentError.new "Method #{method} Not found"
            end
          end
        end
      {% end %}
    end
  end
end