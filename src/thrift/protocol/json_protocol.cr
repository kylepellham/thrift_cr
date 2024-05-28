require "base64"
require "json"
require "./base_protocol.cr"

module Thrift
  struct ::Char
    def as_bytes
      Bytes.new(bytes.to_unsafe, bytesize)
    end
  end

  class LookaheadReader

    @data : Char?
    def initialize(trans : ::Thrift::BaseTransport)
      @trans = trans
      @hasData = false
      @data = nil
    end

    def read
      if @hasData
        @hasData = false
      else
        @data = @trans.read_char
      end

      return @data
    end

    def peek
      if !@hasData
        @data = @trans.read_char
      end
      @hasData = true
      return @data
    end
  end

  #
  # Class to serve as base JSON context and as base class for other context
  # implementations
  #
  class JSONContext
    JSON_ELEM_SEPARATOR = ','
    #
    # Write context data to the trans. Default is to do nothing.
    #
    def write(trans)
    end

    #
    # Read context data from the trans. Default is to do nothing.
    #
    def read(reader)
    end

    #
    # Return true if numbers need to be escaped as strings in this context.
    # Default behavior is to return false.
    #
    def escape_num
      return false
    end
  end

  # Context class for object member key-value pairs
  class JSONPairContext < JSONContext
    JSON_PAIR_SEPARATOR = ':'

    def initialize
      @first = true
      @colon = true
    end

    def write(trans)
      if (@first)
        @first = false
        @colon = true
      else
        trans.write((@colon ? JSON_PAIR_SEPARATOR : JSON_ELEM_SEPARATOR).as_bytes)
        @colon = !@colon
      end
    end

    def read(reader)
      if (@first)
        @first = false
        @colon = true
      else
        ch = (@colon ? JSON_PAIR_SEPARATOR : JSON_ELEM_SEPARATOR)
        @colon = !@colon
        JsonProtocol.read_syntax_char(reader, ch)
      end
    end

    # Numbers must be turned into strings if they are the key part of a pair
    def escape_num
      return @colon
    end
  end

  # Context class for lists
  class JSONListContext < JSONContext

    def initialize
      @first = true
    end

    def write(trans)
      if (@first)
        @first = false
      else
        trans.write(JSON_ELEM_SEPARATOR.as_bytes)
      end
    end

    def read(reader)
      if (@first)
        @first = false
      else
        JsonProtocol.read_syntax_char(reader, JSON_ELEM_SEPARATOR)
      end
    end
  end

  class JsonProtocol < BaseProtocol
    THRIFT_VERSION1 = 1
    JSON_OBJECT_START = '{'
    JSON_OBJECT_END = '}'
    JSON_ARRAY_START = '['
    JSON_ARRAY_END = ']'
    JSON_NEWLINE = '\n'
    JSON_BACKSLASH = '\\'
    JSON_STRING_DELIMITER = '"'

    THRIFT_NAN = "NaN"
    THRIFT_INFINITY = "Infinity"
    THRIFT_NEGATIVE_INFINITY = "-Infinity"


    def initialize(trans)
      super(trans)
      @context = JSONContext.new
      @contexts = [] of JSONContext
      @reader = LookaheadReader.new(trans)
    end

    def get_json_name_from_type(ttype)
      case ttype
      when Types::Bool
        "tf"
      when Types::Byte
        "i8"
      when Types::I16
        "i16"
      when Types::I32
        "i32"
      when Types::I64
        "i64"
      when Types::Double
        "dbl"
      when Types::String
        "str"
      when Types::Struct
        "rec"
      when Types::Map
        "map"
      when Types::Set
        "set"
      when Types::List
        "lst"
      else
        raise NotImplementedError.new ""
      end
    end

    def get_type_from_json_name(name)
      case name
      when "tf"
        result = Types::Bool
      when "i8"
        result = Types::Byte
      when "i16"
        result = Types::I16
      when "i32"
        result = Types::I32
      when "i64"
        result = Types::I64
      when "dbl"
        result = Types::Double
      when "str"
        result = Types::String
      when "rec"
        result = Types::Struct
      when "map"
        result = Types::Map
      when "set"
        result = Types::Set
      when "lst"
        result = Types::List
      else
        result = Types::Stop
      end
      if (result == Types::Stop)
        raise NotImplementedError.new ""
      end
      return result
    end

    # Static helper functions

    # Read 1 character from the trans and verify that it is the expected character ch.
    # Throw a protocol exception if it is not.
    def self.read_syntax_char(reader, ch)
      ch2 = reader.read
      if (ch2 != ch)
        raise ProtocolException.new(ProtocolException::INVALID_DATA, "Expected \"#{ch}\" got \'#{ch2}\'.")
      end
    end

   # Return true if the character ch is in [-+0-9.Ee]; false otherwise
    def is_json_numeric(ch)
      case ch
      when Nil
        return false
      when '+', '-', '.', ('0'..'9'), 'E', "e"
        return true
      else
        return false
      end
    end

    def push_context(context)
      @contexts.push(@context)
      @context = context
    end

    def pop_context
      @context = @contexts.pop
    end

    # Write the character ch as a JSON escape sequence ("\u00xx")
    def write_json_escape_char(ch)
      trans.write("\\u".to_slice)
      ch_value = ch[0]
      if (ch_value.kind_of? String)
        ch_value = ch.bytes.first
      end
      trans.write(ch_value.to_s(16).rjust(4,'0'))
    end

    # This table describes the handling for the first 0x30 characters
    # 0 : escape using "\u00xx" notation
    # 1 : just output index
    # <other> : escape using "\<other>" notation
    JSON_CHAR_TABLE = [
        # 0 1 2 3 4 5 6 7 8 9 A B C D E F
        0, 0, 0, 0, 0, 0, 0, 0,'b','t','n', 0,'f','r', 0, 0, # 0
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, # 1
        1, 1,'"', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, # 2
    ]

    # Write the character ch as part of a JSON string, escaping as appropriate.
    def write_json_char(ch)
      ch_value = ch.unsafe_as(UInt8)
      if (ch_value >= 0x30)
        if (ch == JSON_BACKSLASH) # Only special character >= 0x30 is '\'
          trans.write(JSON_BACKSLASH.as_bytes)
          trans.write(JSON_BACKSLASH.as_bytes)
        else
          trans.write(ch.as_bytes)
        end
      else
        outCh = JSON_CHAR_TABLE[ch_value];
        # Check if regular character, backslash escaped, or JSON escaped
        if outCh.kind_of? String
          trans.write_byte(JSON_BACKSLASH)
          trans.write_byte(outCh.unsafe_as(UInt8))
        elsif outCh == 1
          trans.write_byte(ch.unsafe_as(UInt8))
        else
          write_json_escape_char(ch)
        end
      end
    end

    # Write out the contents of the string str as a JSON string, escaping characters as appropriate.
    def write_json_string(str)
      @context.write(trans)
      trans.write(JSON_STRING_DELIMITER.as_bytes)
      trans.write_string(str.to_slice)
      trans.write(JSON_STRING_DELIMITER.as_bytes)
    end

    # Write out the contents of the string as JSON string, base64-encoding
    # the string's contents, and escaping as appropriate
    def write_json_base64(bytes)
      @context.write(trans)
      trans.write(JSON_STRING_DELIMITER.as_bytes)
      Base64.strict_encode(bytes, trans)
      trans.write(JSON_STRING_DELIMITER.as_bytes)
    end

    # Convert the given integer type to a JSON number, or a string
    # if the context requires it (eg: key in a map pair).
    def write_json_integer(num)
      @context.write(trans)
      escape_num = @context.escape_num
      if (escape_num)
        trans.write(JSON_STRING_DELIMITER.as_bytes)
      end
      trans.write_string(num.to_s.to_slice);
      if (escape_num)
        trans.write(JSON_STRING_DELIMITER.as_bytes)
      end
    end

    # Convert the given double to a JSON string, which is either the number,
    # "NaN" or "Infinity" or "-Infinity".
    def write_json_double(num)
      @context.write(trans)
      # Normalize output of thrift::to_string for NaNs and Infinities
      special = false;
      if (num.nan?)
        special = true;
        val = THRIFT_NAN;
      elsif (num.infinite?)
        special = true;
        val = THRIFT_INFINITY;
        if (num < 0.0)
          val = THRIFT_NEGATIVE_INFINITY;
        end
      else
        val = num.to_s
      end

      escape_num = special || @context.escape_num
      if (escape_num)
        trans.write(JSON_STRING_DELIMITER.as_bytes)
      end
      trans.write_string(val.to_slice)
      if (escape_num)
        trans.write(JSON_STRING_DELIMITER.as_bytes)
      end
    end

    def write_json_object_start
      @context.write(trans)
      trans.write(JSON_OBJECT_START.as_bytes)
      push_context(JSONPairContext.new);
    end

    def write_json_object_end
      pop_context
      trans.write(JSON_OBJECT_END.as_bytes)
    end

    def write_json_array_start
      @context.write(trans)
      trans.write(JSON_ARRAY_START.as_bytes)
      push_context(JSONListContext.new);
    end

    def write_json_array_end
      pop_context
      trans.write(JSON_ARRAY_END.as_bytes)
    end

    def write_message_begin(name : String, type : Thrift::MessageTypes, seqid : Int32)
      write_json_array_start
      write_json_integer(THRIFT_VERSION1)
      write_json_string(name)
      write_json_integer(type.to_i)
      write_json_integer(seqid)
    end

    def write_message_end
      write_json_array_end
    end

    def write_struct_begin(name)
      write_json_object_start
    end

    def write_struct_end
      write_json_object_end
    end

    def write_field_begin(name : String, type : Thrift::Types, id : Int16)
      write_json_integer(id)
      write_json_object_start
      write_json_string(get_json_name_from_type(type))
    end

    def write_field_end
      write_json_object_end
    end

    def write_field_stop
      nil
    end

    def write_map_begin(ktype, vtype, size)
      write_json_array_start
      write_json_string(get_json_name_from_type(ktype))
      write_json_string(get_json_name_from_type(vtype))
      write_json_integer(size)
      write_json_object_start
    end

    def write_map_end
      write_json_object_end
      write_json_array_end
    end

    def write_list_begin(etype, size)
      write_json_array_start
      write_json_string(get_json_name_from_type(etype))
      write_json_integer size
    end

    def write_list_end
      write_json_array_end
    end

    def write_set_begin(etype, size)
      write_list_begin(etype, size)
    end

    def write_set_end
      write_list_end
    end

    def write_bool(bool : Bool)
      write_json_integer(bool ? 1 : 0)
    end

    def write_byte(byte : Int8)
      write_json_integer(byte)
    end

    def write_i16(i16 : Int16)
      write_json_integer(i16)
    end

    def write_i32(i32 : Int32)
      write_json_integer(i32)
    end

    def write_i64(i64 : Int64)
      write_json_integer(i64)
    end

    def write_double(dub : Float64)
      write_json_double(dub)
    end

    def write_uuid(uuid : UUID)
      write_json_string(uuid.to_s)
    end

    def write_string(str : String)
      write_json_string(str)
    end

    def write_binary(buf : Bytes)
      write_json_base64(buf)
    end

    ##
    # Reading functions
    ##

    # Reads 1 byte and verifies that it matches ch.
    def read_json_syntax_char(ch)
      JsonProtocol.read_syntax_char(@reader, ch)
    end

    # Decodes the four hex parts of a JSON escaped string character and returns
    # the character via out.
    #
    # Note - this only supports Unicode characters in the BMP (U+0000 to U+FFFF);
    # characters above the BMP are encoded as two escape sequences (surrogate pairs),
    # which is not yet implemented
    def read_json_escape_char
      StaticArray(UInt8, 4).new do |i|
        case ch = @reader.read
        when .nil?
          0_u8
        else
          ch.unsafe_as(UInt8)
        end
      end.unsafe_as(Char)
    end

    # Decodes a JSON string, including unescaping, and returns the string via str
    def read_json_string(skipContext = false)
      # This string's characters must match up with the elements in escape_char_vals.
      # I don't have '/' on this list even though it appears on www.json.org --
      # it is not in the RFC -> it is. See RFC 4627
      escape_chars = "\"\\/bfnrt"

      # The elements of this array must match up with the sequence of characters in
      # escape_chars
      escape_char_vals = [
        "\"", "\\", "\/", "\b", "\f", "\n", "\r", "\t",
      ]

      if !skipContext
        @context.read(@reader)
      end
      read_json_syntax_char(JSON_STRING_DELIMITER)
      ch = ""
      str = ""
      while (true)
        ch = @reader.read
        if (ch == JSON_STRING_DELIMITER)
          break
        end
        if (ch == JSON_BACKSLASH)
          ch = @reader.read
          if (ch == 'u')
            ch = read_json_escape_char
          else
            pos = nil
            pos = escape_chars.index(ch) if ch;
            if (pos.nil?) # not found
              raise ProtocolException.new(ProtocolException::INVALID_DATA, "Expected control char, got \'#{ch}\'.")
            end
            ch = escape_char_vals[pos]
          end
        end
        str += ch if ch
      end
      return str
    end

    # Reads a block of base64 characters, decoding it, and returns via str
    def read_json_base64
      str = read_json_string
      m = str.length % 4
      if m != 0
        # Add missing padding
        (4 - m).times do
          str += '='
        end
      end
      Base64.decode(str).to_slice
    end

    # Reads a sequence of characters, stopping at the first one that is not
    # a valid JSON numeric character.
    def read_json_numeric_chars
      str = ""
      loop do
        ch = @reader.peek
        if (!is_json_numeric(ch))
          break;
        end
        ch = @reader.read
        str += ch if ch
      end
      return str
    end

    # Reads a sequence of characters and assembles them into a number,
    # returning them via num
    def read_json_integer
      @context.read(@reader)
      if (@context.escape_num)
        read_json_syntax_char(JSON_STRING_DELIMITER)
      end
      str = read_json_numeric_chars
      begin
        num = str.to_i;
      rescue
        raise ProtocolException.new(ProtocolException::INVALID_DATA, "Expected numeric value; got \"#{str}\"")
      end

      if (@context.escape_num)
        read_json_syntax_char(JSON_STRING_DELIMITER)
      end

      return num
    end

    # Reads a JSON number or string and interprets it as a double.
    def read_json_double
      @context.read(@reader)
      num = 0
      if (@reader.peek == JSON_STRING_DELIMITER)
        str = read_json_string(true)
        # Check for NaN, Infinity and -Infinity
        if (str == THRIFT_NAN)
          num = Float64::NAN
        elsif (str == THRIFT_INFINITY)
          num = Float64::INFINITY
        elsif (str == THRIFT_NEGATIVE_INFINITY)
          num = -Float64::INFINITY
        else
          if (!@context.escape_num)
            # Raise exception -- we should not be in a string in this case
            raise ProtocolException.new(ProtocolException::INVALID_DATA, "Numeric data unexpectedly quoted")
          end
          begin
            num = str.to_f64
          rescue
            raise ProtocolException.new(ProtocolException::INVALID_DATA, "Expected numeric value; got \"#{str}\"")
          end
        end
      else
        if (@context.escape_num)
          # This will throw - we should have had a quote if escape_num == true
          read_json_syntax_char(JSON_STRING_DELIMITER)
        end
        str = read_json_numeric_chars
        begin
          num = str.to_f64
        rescue
          raise ProtocolException.new(ProtocolException::INVALID_DATA, "Expected numeric value; got \"#{str}\"")
        end
      end
      return num
    end

    def read_json_object_start
      @context.read(@reader)
      read_json_syntax_char(JSON_OBJECT_START)
      push_context(JSONPairContext.new)
      nil
    end

    def read_json_object_end
      read_json_syntax_char(JSON_OBJECT_END)
      pop_context
      nil
    end

    def read_json_array_start
      @context.read(@reader)
      read_json_syntax_char(JSON_ARRAY_START)
      push_context(JSONListContext.new)
      nil
    end

    def read_json_array_end
      read_json_syntax_char(JSON_ARRAY_END)
      pop_context
      nil
    end

    def read_message_begin : Tuple(String, Thrift::MessageTypes, Int32)
      read_json_array_start
      version = read_json_integer
      if (version != THRIFT_VERSION1)
        raise ProtocolException.new(ProtocolException::BAD_VERSION, "Message contained bad version.")
      end
      name = read_json_string
      message_type = ::Thrift::MessageTypes.from_value(read_json_integer)
      seqid = read_json_integer
      return name, message_type, seqid
    end

    def read_message_end
      read_json_array_end
      nil
    end

    def read_struct_begin
      read_json_object_start
    end

    def read_struct_end
      read_json_object_end
      nil
    end

    def read_field_begin : Tuple(String, Thrift::Types, Int16)
      # Check if we hit the end of the list
      ch = @reader.peek
      if (ch == JSON_OBJECT_END)
        field_type = Types::Stop
        return "", field_type, 0_i16
      else
        field_id = read_json_integer.to_i16
        read_json_object_start
        field_type = get_type_from_json_name(read_json_string)

        return "", field_type, field_id
      end
    end

    def read_field_end
      read_json_object_end
    end

    def read_map_begin : Tuple(Thrift::Types, Thrift::Types, Int32)
      read_json_array_start
      ktype = get_type_from_json_name(read_json_string)
      vtype = get_type_from_json_name(read_json_string)
      size = read_json_integer
      read_json_object_start

      return ktype, vtype, size
    end

    def read_map_end
      read_json_object_end
      read_json_array_end
    end

    def read_list_begin : Tuple(Thrift::Types, Int32)
      read_json_array_start
      etype = get_type_from_json_name(read_json_string)
      size = read_json_integer

      return etype, size
    end

    def read_list_end
      read_json_array_end
    end

    def read_set_begin : Tuple(Thrift::Types, Int32)
      read_list_begin
    end

    def read_set_end
      read_list_end
    end

    def read_bool : Bool
      read_byte != 0
    end

    def read_byte : Int8
      read_json_integer.to_i8
    end

    def read_i16 : Int16
      read_json_integer.to_i16
    end

    def read_i32 : Int32
      read_json_integer
    end

    def read_i64 : Int64
      read_json_integer.to_i64
    end

    def read_double : Float64
      read_json_double
    end

    def read_uuid : UUID
      UUID.new read_json_string
    end

    def read_string : String
      read_json_string
    end

    def read_binary : Bytes
      Base64.decode(reader.read_string)
    end
  end

  class JsonProtocolFactory < BaseProtocolFactory
    def get_protocol(trans)
      ::Thrift::JsonProtocol.new(trans)
    end

    def to_s
      "json"
    end
  end
end