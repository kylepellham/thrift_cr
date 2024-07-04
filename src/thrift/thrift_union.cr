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

require "./thrift_struct.cr"

module Thrift
  module Union

    # Internal annotation to identify union variables
    annotation UnionVar
    end

    # inherited macro to define a property for a union
    #
    # ```
    # union_property x : Int32
    # ```
    #
    # is equivalent to
    #
    # ```
    # @[UnionVar]
    # def x : Int32
    #   @storage_internal.as(Int32)
    # end
    #
    # def x=(@storage_internal : Int32)
    # end
    # ```
    macro union_property(name)
      {% if name.is_a?(TypeDeclaration) %}
        {% if name.value %}
          {{raise "Unions Cannot Have Default Values"}}
        {% end %}
        @[UnionVar]
        def {{name.var.id}} : {{name.type.id}}
          return @storage.unsafe_as({{name.type.id}})
        end
        def {{name.var.id}}=({{name.var.id}}_val : {{name.type.id}})
          @storage = {{name.var.id}}_val
        end
        def is_{{name.var.id}}
          @storage.is_a?({{name.type.id}})
        end
      {% else %}
        {{ raise "Needs to be Type Declaration ex: union_property x : Int32" }}
      {% end %}
    end

    # union are to be initialized with key-word arg to initialize a union
    #
    # ```
    # class MyUnion
    #   include ::Thrift::Union
    #   union_property x : Int32
    #   union_property y : String
    # end
    #
    # a_union = MyUnion.new(x: 32532)
    # a_union.x # => 32532
    # other_union = MyUnion.new(y: "hello")
    # other_union.y # => "hello
    # ```
    def initialize(**kwargs)
      if kwargs.size > 1
        raise ArgumentError.new "Expected 1 Argument #{kwargs.size} Given"
      end
      # default values are okay because at most only one of these conditions can be true
      keys = kwargs.keys
      {% for var in @type.methods.select(&.annotation(::Thrift::Union::UnionVar)) %}
        if keys.includes?({{var.name.symbolize}})
          @storage = kwargs.fetch({{var.name.symbolize}}, nil.unsafe_as({{var.return_type.id}}))
        end
      {% end %}
    end

    def <=>(other : self)
      if @storage.class == other.@storage.class
        return 0 if @storage.nil?
        return @storage <=> other.@storage
      end
      return -1 if !@storage.nil? && other.@storage.nil?
      return 1 if @storage.nil? && other.@storage.nil?
      #really not much else we can compare at this point
      return @storage.class.name <=> other.@storage.class.name
    end

    # method that will write a Thrift Union to a Transport encoding in the Given Protocol
    #
    # ```
    # require "thrift"
    #
    # class MyUnion
    #   include ::Thrift::Union
    #
    #   @[::Thrift::Type::SerialOpts(fid: 0)]
    #   union_property prop_int : Int32
    #   @[::Thrift::Type::SerialOpts(fid: 1)]
    #   union_property prop_str : String
    # end
    #
    # transport = Thrift::Transport::MemoryBufferTransport.new
    # protocol = Thrift::Protocol::BinaryProtocol.new(transport)
    #
    # my_union = MyUnion.new(prop_int: 12)
    # my_union.write to: protocol
    # transport.peek # => Bytes[8, 0, 0, 0, 0, 0, 12, 0]
    # ```
    def write(to oprot : ::Thrift::Protocol::BaseProtocol)
      oprot.write_recursion do
        oprot.write_struct_begin({{@type.stringify}})
        {% begin %}
          case @storage
          {% for var in @type.methods.select(&.annotation(UnionVar)) %}
            when .is_a?({{var.return_type}})
              oprot.write_field_begin({{var.name.stringify}}, {{var.name}}.thrift_type, {{var.annotation(::Thrift::Type::SerialOpts)[:fid]}}.to_i16)
              {{var.name}}.write to: oprot
              oprot.write_field_end
          {% end %}
          end
        {% end %}
        oprot.write_field_stop
        oprot.write_struct_end
      end
    end

    # method that will read a Thrift Union from a Transport decoding with the given Protocol
    #
    # ```
    # require "thrift"
    #
    # class MyUnion
    #   include ::Thrift::Union
    #
    #   @[::Thrift::Type::SerialOpts(fid: 0)]
    #   union_property prop_int : Int32
    #   @[::Thrift::Type::SerialOpts(fid: 1)]
    #   union_property prop_str : String
    # end
    #
    # transport = Thrift::Transport::MemoryBufferTransport.new
    # transport.write(Bytes[8, 0, 0, 0, 0, 0, 12, 0])
    # protocol = Thrift::Protocol::BinaryProtocol.new(transport)
    #
    # my_union = MyUnion.read from: protocol
    # my_union.prop_int # => 12
    # my_union.prop_str # => undefined behavior
    # ```
    protected def read(from iprot : ::Thrift::Protocol::BaseProtocol)
      iprot.read_recursion do
        iprot.read_struct_begin
        loop do
          name, ftype, fid = iprot.read_field_begin
          break if ftype == ::Thrift::Types::Stop
          raise "Too Many fields for Union" if union_set?
          {% begin %}
          case {fid, ftype}
            {% for var in @type.methods.select(&.annotation(UnionVar)) %}
              when  { {{var.annotation(::Thrift::Type::SerialOpts)[:fid]}}, {{var.return_type.id}}.thrift_type }
                @storage = {{var.return_type.id}}.read from: iprot
            {% end %}
          else
            iprot.skip ftype
          end
          {% end %}
          iprot.read_field_end
        end
        iprot.read_struct_end
      end
    end

    # the 'included' macro generates the internal storage variable of the union from the fields provided with the union_property macro
    # ex:
    # ```
    # class MyUnion
    #   include ::Thrift::Union
    #
    #   @[::Thrift::Type::SerialOpts(fid: 0)]
    #   union_property prop_int : Int32
    #   @[::Thrift::Type::SerialOpts(fid: 1)]
    #   union_property prop_str : String
    # end
    # ```
    # will generate storage variable of
    # ```
    # @storage : Int32 | String | Nil
    # ```
    macro included
      {% verbatim do %}
        include Comparable(self)
        include ::Thrift::Type
        include ::Thrift::Type::Read
        extend ::Thrift::Type::ClassRead
        define_thrift_type ::Thrift::Struct
        macro finished
          {% begin %}
            {%
              union_vars = @type.methods.select{ |method| method.annotation(UnionVar) }.map{|method| "::#{method.return_type}".id}
            %}
            @storage : {{ union_vars.join(" | ").id }} | Nil
          {% end %}
          def ==(other : self)
            @storage == other.@storage
          end
        end
      {% end %}

      def_hash @storage

      def union_set?
        !@storage.nil?
      end

      def union_internal
        @storage
      end
    end

  end
end
