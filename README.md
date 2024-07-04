# thrift_cr

thrift_cr is library to be used in conjunction with apache thrift in order to use apache thrift generated crystal code

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     thrift:
       github: kylepellham/thrift_cr
   ```

2. Run `shards install`

## Usage

```crystal
require "thrift"
```

All library definitions exist in the namespace `Thrift`. Through Thrift you can initialize a server with a transport:

  - `Thrift::Transport::ServerSocketTransport`

and a protocol factory:

  - `Thrift::Protocol::BinaryProtocolFactory`
  - `Thrift::Protocol::JsonProtocolFactory`

you can mix any factory with any server transport. When you use this library you are expected to use the thrift generation this is how you get a server processor for a service. When you get processor you can create one of the following servers:

  - `Thrift::Server::SimpleServer`

this is an example of creating and running a server:

```crystal
server_transport = Thrift::Transport::Socket.new("localhost", 9090)
trans_factory = Thrift::Transport::SocketTransportFactory.new
prot_factory = Thrift::Protocol::BinaryProtocolFactory.new

# this would be generated from a thrift idl file
handler = MyHandler.new
processor = MyService::Processor(MyHandler).new(handler)

server = Thrift::Server::SimpleServer.new(processor, server_transport, trans_factory, prot_factory)
```

## Contributing

1. Fork it (<https://github.com/kylepellham/thrift_cr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Kyle Pellham](https://github.com/kylepellham) - creator and maintainer
