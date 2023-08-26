require "./protocol/base_protocol.cr"
require "./types.cr"

module Thrift
  module Client

    @iprot : BaseProtocol
    @oprot : BaseProtocol
    @seqid : Int32 = 0 

    def initialize(iprot, oprot = nil)
      @iprot = iprot
      @oprot = oprot || iprot
    end

    def send_message(name, type : ArgsClass.class, args = {} of String => String) forall ArgsClass
      @oprot.write_message_begin(name, MessageTypes::CALL, @seqid)
      send_message_args(args_class, args)
    end

    def send_oneway_message(name, type : ArgsClass.class, args = {} of String => String) forall ArgsClass
      @oprot.write_message_begin(name, MessageTypes::ONEWAY, @seqid)
      send_message_args(args_class, args)
    end

    def send_message_args(type : ArgsClass.class, args) forall ArgsClass
      data = ArgsClass.new
      args.each do |k, v|
        data.send("#{k.to_s}=", v)
      end
      begin
        data.write(@oprot)
      rescue ex : Exception
        @oprot.trans.close
        raise ex
      end
      @oprot.write_message_end
      @oprot.trans.flush
    end

    def receive_message_begin()
      fname, mtype, rseqid = @iprot.read_message_begin
      [fname, mtype, rseqid]
    end

    def reply_seqid(rseqid)
     result = (rseqid==@seqid) ? true : false
     result
    end

    def receive_message(type : ResultKlass.class) : ResultKlass forall ResultKlass
      result = ResultKlass.new
      result.read(@iprot)
      @iprot.read_message_end
      result
    end

    def handle_exception(mtype)
      if mtype == MessageTypes::EXCEPTION
        x = ApplicationException.new
        x.read(@iprot)
        @iprot.read_message_end
        raise x
      end
    end
  end
end
