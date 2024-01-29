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
  class MemoryBufferTransport < BaseTransport
    GARBAGE_BUFFER_SIZE = 0x1 << 12 # 4kB

     @buf : Bytes
     @index : Int32
    def initialize(buffer : Bytes? = nil)
      @buf = buffer ? buffer : Bytes.empty
      @index = 0
    end

    def open?
      return true
    end

    def open
    end
    
    def close
    end

    def peek
      @index < @buf.size
    end

    def reset_buffer(new_buffer : Bytes)
      @buf = new_buffer
      @index = 0
    end

    def available
      @buf.size - @index
    end

    def read(len)
      data = @buf[@index..@index + len - 1]
      @index += len
      @index = @buf.size if @index > @buf.size
      if @index >= GARBAGE_BUFFER_SIZE
        @buf = @buf[@index..-1]
        @index = 0
      end
      if data.size < len
        raise IO::Error.new "Not Enough Bytes Remain in buffer"
      end
      data
    end

    def read_byte
      raise IO::Error.new "Not Enough Bytes Remain in buffer" if @index >= @buf.size
      val = @buf[@index]
      @index += 1
      if @index >= GARBAGE_BUFFER_SIZE
        @buf = @buf[@index..-1]
        @index = 0
      end
      val
    end

    def read_into_buffer(buffer, size)
      i = 0
      while i < size
        raise IO::Error.new "Not enough bytes remain in buffer" if @index >= @buf.size

        byte = @buf[@index]
        buffer[i] = byte
        @index += 1
        i += 1
      end

      if @index >= GARBAGE_BUFFER_SIZE
        @buf = @buf[@index..-1]
        @index = 0
      end
      i
    end

    def write(wbuf : Bytes)
      @buf = @buf.join_with(wbuf)
    end

    def flush
    end

    def to_s
      "memory"
    end
  end
end


