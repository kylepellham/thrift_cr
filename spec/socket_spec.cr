require "./spec_helper"

describe "Socket" do

  describe Thrift::Socket do
    socket : Thrift::Socket
    handle : TCPSocket?

    before_each do
      socket = Thrift::Socket.new
      handle = socket.handle
    end
  end
end