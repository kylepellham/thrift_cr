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

require "log"
require "./protocol/base_protocol.cr"
require "./types.cr"
require "./exceptions.cr"

module Thrift
  module Processor
    Log = ::Log.for(Processor)
    def initialize(handler)
      @handler = handler
    end

    def read_args(iprot, args_class : ArgsClass.class) forall ArgsClass
      args = ArgsClass.read(iprot)
      iprot.read_message_end
      args
    end

    def write_result(result, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::Reply, seqid)
      result.write to: oprot
      oprot.write_message_end
      oprot.trans.flush
    end

    def write_error(err, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::Exception, seqid)
      err.write to: oprot
      oprot.write_message_end
      oprot.trans.flush
    end

    def process(iprot : Thrift::Protocol::BaseProtocol, oprot : Thrift::Protocol::BaseProtocol)
      name, type, seqid = iprot.read_message_begin
      begin
        {% begin %}
        case name.underscore
        {% for method in @type.methods %}
          {% if method.name.stringify[0.."process_".size - 1] == "process_" %}
        when "{{method.name["process_".size..-1].id}}"
          {{method.name}}(seqid, iprot, oprot)
          {% end %}
        {% end %}
        else
          iprot.skip(::Thrift::Types::Struct)
          iprot.read_message_end
          x = ::Thrift::ApplicationException.new(::Thrift::ApplicationException::UNKNOWN_METHOD, "Unknown function " + name)
          write_error(x, oprot, name, seqid)
          return false
        end
        {% end %}
        return true
      rescue ex
        x = ::Thrift::ApplicationException.new(::Thrift::ApplicationException::INTERNAL_ERROR, "Internal error")
        Log.debug { "Internal error : #{ex.message}\n#{ex.backtrace.join("\n")}" }
        write_error(x, oprot, name, seqid)
        return false
      end
    end
  end
end
