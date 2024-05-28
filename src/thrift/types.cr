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

    # annotation to hold thrift metadata. required on all thrift type properties
    # required fields:
    #   fid - id value for writing and reading
    #   requirement - :optional, :requirement, :opt_in_req_out
    #   transmit_name - (optional) appears when the idl name was not safe for crystal
    annotation Properties
    end

    # All thrift compatible types need to define a write method
    abstract def write(to oprot : ::Thrift::BaseProtocol)

    # this module is indirectly extended in in include macros
    module Read
      # All thrift compatible types need to define a read method
      abstract def read(from iprot : ::Thrift::BaseProtocol)
    end

    # mixin module to define a class level read method
    module ClassRead
      def read(from iprot : ::Thrift::BaseProtocol)
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
