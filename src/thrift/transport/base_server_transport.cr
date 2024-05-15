module Thrift
  abstract class BaseServerTransport
    abstract def listen

    abstract def accept?

    abstract def closed?

    def accept(&)
      if client = accept?
        begin
          yield client
        ensure
          client.close
        end
      end
    end

    def close
      nil
    end
  end
end
