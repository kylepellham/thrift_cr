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
    module Write
      abstract def write(to oprot : ::Thrift::BaseProtocol)
    end

    module Read
      abstract def read(from iprot : ::Thrift::BaseProtocol)
    end

    module ClassRead
      def read(from iprot : ::Thrift::BaseProtocol)
        obj = self.allocate
        obj.tap(&.read from: iprot)
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
        include ::Thrift::Type::Write
        extend ::Thrift::Type::Read
      {% end %}
    end

    macro extended
      {{raise "can only include ::Thrift::Type"}}
    end
  end
end
