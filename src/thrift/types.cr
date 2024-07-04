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

module Thrift
  enum Types : Int8
    Stop   =  0
    Void   =  1
    Bool   =  2
    Byte   =  3
    Double =  4
    I16    =  6
    I32    =  8
    I64    = 10
    String = 11
    Struct = 12
    Map    = 13
    Set    = 14
    List   = 15
    Uuid   = 16
  end

  enum MessageTypes : Int8
    Call      = 1
    Reply     = 2
    Exception = 3
    Oneway    = 4
  end

  module Type

    # annotation to hold thrift metadata. required on all thrift type SerialOpts
    # required fields:
    # ```text
    #   fid - id value for writing and reading
    #   requirement - :optional, :required, :opt_in_req_out
    #   transmit_name - (optional) appears when the idl name was not safe for crystal
    # ```
    annotation SerialOpts
    end

    # All thrift compatible types need to define a write method
    abstract def write(to oprot : ::Thrift::Protocol::BaseProtocol)

    # this module is indirectly extended in include macros
    module Read
      # All thrift compatible types need to define a class level read method
      abstract def read(from iprot : ::Thrift::Protocol::BaseProtocol)
    end

    # mixin module to define a class level read method
    module ClassRead
      # read method for reading an object from a transport using a given protocol
      #
      # ```
      # require "thrift"
      #
      # class MyClass
      #   include Thrift::Struct
      #
      #   @[Thrift::Type::SerialOpts(fid: 0, requirement: :required)]
      #   struct_property prop_int : Int32
      #   @[Thrift::Type::SerialOpts(fid: 1, requirement: :required)]
      #   struct_property prop_str : String
      #
      #   def initialize(@prop_int, @prop_str)
      #   end
      # end
      #
      # transport = Thrift::Transport::MemoryBufferTransport.new
      # protocol = Thrift::Protocol::BinaryProtocol.new(transport)
      # transport.write(Bytes[8, 0, 0, 0, 0, 0, 12, 11, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0])
      #
      # my_class = MyClass.read from: protocol
      # my_class.prop_int # => 12
      # my_class.prop_str # => "hello"
      # ```
      def read(from iprot : ::Thrift::Protocol::BaseProtocol)
        obj = self.allocate
        obj.tap(&.read from: iprot)
      end

      macro included
        {{raise "Can only include Thrift::Type::ClassRead"}}
      end
    end

    macro define_thrift_type(thrift_type)
      def self.thrift_type
        {{thrift_type}}
      end

      def thrift_type
        {{thrift_type}}
      end
    end

    macro included
      {% verbatim do %}
        extend ::Thrift::Type::Read
      {% end %}
    end

    macro extended
      {{raise "can only include ::Thrift::Type"}}
    end
  end
end
