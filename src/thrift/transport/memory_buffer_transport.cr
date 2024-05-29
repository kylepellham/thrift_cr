# encoding: ascii-8bit
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
  # Crystal has a built in memory buffer IO but we need to handle indexing different
  class MemoryBufferTransport < BaseTransport
    DEFAULT_BUFFER = 0x1 << 12 # 4kB

    @buf : Pointer(UInt8)
    @pos : Int32
    @total_bytes : Int32

    def initialize(@capacity : Int32 = DEFAULT_BUFFER)
      @buf = Pointer(UInt8).malloc(@capacity)
      @pos = 0
      @total_bytes = 0
    end

    def open?
      return true
    end

    def open
    end

    def close
    end

    def peek
      if @total_bytes <= @pos
        nil
      else
        Slice.new(@buf + @pos, @total_bytes - @pos)
      end
    end

    def reset_buffer
      @pos = 0
      @total_bytes = 0
    end

    def available
      @total_bytes - @pos
    end

    def read(slice : Bytes)
      # return 0 if slice.empty?
      slice.copy_from(@buf + @pos, slice.size)
      @pos += slice.size
      @pos = @total_bytes if @pos > @total_bytes
      if @pos >= @capacity
        @buf.move_from(@buf + @pos - @capacity, @pos - @capacity)
        @pos = 0
      end
      slice.size
    rescue
      raise IO::Error.new "Not Enough Bytes Remain in buffer"
    end

    def read_byte
      pos = Math.min(@pos, @total_bytes)
      if pos >= @total_bytes
        nil
      else
        val = @buf[@pos]
        @pos += 1
        val
      end
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new "Overflow" if @total_bytes + slice.size > @capacity
      slice.move_to(@buf + @total_bytes, slice.size)
      @total_bytes += slice.size
    end

    def flush
    end

    def to_s
      "memory"
    end
  end
end
