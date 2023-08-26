module Thrift
  class BaseServerTransport
    def listen
      raise NotImplementedError.new ""
    end

    def accept
      raise NotImplementedError.new ""
    end
      
    def close; nil; end

    def closed?
      raise NotImplementedError.new ""
    end
  end
end
