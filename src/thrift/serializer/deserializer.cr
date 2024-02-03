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

require "../protocol/binary_protocol.cr"
require "../transport/memory_buffer_transport.cr"

module Thrift
  class Deserializer
    @protocol : BaseProtocol

    def initialize(protocol_factory = BinaryProtocolFactory.new)
      @transport = MemoryBufferTransport.new
      @protocol = protocol_factory.get_protocol(@transport)
    end

    def deserialize(buffer, otype : Type.class) forall Type
      @transport.reset_buffer(buffer)
      Type.read(@protocol)
    end

    def self.deserialize(buffer, otype : Type.class, protocol = BinaryProtocolFactory.new) forall Type
      tmp = new(protocol)
      tmp.deserialize(buffer, otype)
    end
  end
end
