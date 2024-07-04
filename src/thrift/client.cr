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

require "./protocol/base_protocol.cr"
require "./types.cr"

module Thrift
  module Client
    @iprot : Protocol::BaseProtocol
    @oprot : Protocol::BaseProtocol
    @seqid : Int32 = 0

    def initialize(iprot, oprot = nil)
      @iprot = iprot
      @oprot = oprot || iprot
    end

    def send_message(name, type : ArgsClass.class, **args) forall ArgsClass
      @oprot.write_message_begin(name, MessageTypes::Call, @seqid)
      send_message_args(ArgsClass, **args)
    end

    def send_oneway_message(name, type : ArgsClass.class, **args) forall ArgsClass
      @oprot.write_message_begin(name, MessageTypes::Oneway, @seqid)
      send_message_args(ArgsClass, **args)
    end

    def send_message_args(type : ArgsClass.class, **args) forall ArgsClass
      data = ArgsClass.new(**args)
      begin
        data.write to: @oprot
      rescue ex : Exception
        @oprot.trans.close
        raise ex
      end
      @oprot.write_message_end
      @oprot.trans.flush
    end

    def receive_message_begin
      fname, mtype, rseqid = @iprot.read_message_begin
      return fname, mtype, rseqid
    end

    def reply_seqid(rseqid)
      result = (rseqid == @seqid) ? true : false
      result
    end

    def receive_message(type : ResultClass.class) : ResultClass forall ResultClass
      result = ResultClass.read from: @iprot
      @iprot.read_message_end
      result
    end

    def handle_exception(mtype)
      if mtype == MessageTypes::Exception
        ex = ApplicationException.read from: @iprot
        @iprot.read_message_end
        raise ex
      end
    end
  end
end