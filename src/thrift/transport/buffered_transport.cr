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

require "./base_transport.cr"
require "../helpers.cr"

module Thrift
  module Transport
    # BufferedTransport wraps around any transport and makes them buffered
    class BufferedTransport < BaseTransport
      include IO::Buffered

      @transport : BaseTransport
      def initialize(@transport)
      end

      def unbuffered_read(slice : Bytes)
        @transport.read(slice)
      end

      def unbuffered_write(slice : Bytes)
        @transport.write(slice)
      end

      def unbuffered_flush
        @transport.flush
      end

      def unbuffered_close
        @transport.close
      end

      def unbuffered_rewind
        @pos = 0
      end

      def open?
        return @transport.open?
      end

      def to_s
        "buffered(#{@transport.to_s})"
      end
    end

    class BufferedTransportFactory < BaseTransportFactory
      def get_transport(transport)
        return BufferedTransport.new(transport)
      end

      def to_s
        "buffered"
      end
    end
  end
end
