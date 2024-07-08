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

require "./thrift_logging.cr"
require "./types.cr"
require "./helpers.cr"

module Thrift

  # mixing module that will define a class level read, protected instance level read and an instance level write method
  # macro struct_propety is included that MUST be used when defining SerialOpts on a thrift generated class
  module Struct

    # struct_property defines a thrift compatible property of a struct
    #
    # ```
    # struct_property x : Int32
    # ```
    # will generate
    # ```
    # def x : Int32
    #   @x
    # end
    #
    # @x : Int32
    #
    # def x=(x : Int32)
    #   # NOTE: this will generate code based on requirement of this property
    #   @x = x
    # end
    # ```
    macro struct_property(name)
      def {{name.var.id}} : {{name.type.id}}
        @{{name.var.id}}
      end

      @{{name}}

      def {{name.var.id}}=({{name.var.id}})
        \{% if (annotated_getter = @type.methods.find{|method| method.name.symbolize == {{name.var.symbolize}} }) && annotated_getter.annotation(::Thrift::Type::SerialOpts) && annotated_getter.annotation(::Thrift::Type::SerialOpts)[:requirement] == :optional %}
          @__isset.{{name.var.id}} = !{{name.var.id}}.nil?
        \{% end %}
        @{{name.var.id}} = {{name.var.id}}
      end
    end

    def <=>(other : self)
      {% for var in @type.instance_vars %}
      if @{{var.name.id}}.nil? && !other.@{{var.name.id}}.nil?
        return 1
      elsif !@{{var.name.id}}.nil? && other.@{{var.name.id}}.nil?
        return -1
      else
        if ({{var.name.id}} = @{{var.name.id}}) && ({{var.name.id}}_other = other.@{{var.name.id}})
          if ({{var.name.id}}.responds_to?(:<=>))
            cmp = {{var.name.id}} <=> {{var.name.id}}_other
            return cmp if cmp != 0
          end
        end
      end
      {% end %}
      0
    end

    # writes thrift struct to transport encoding with protocol
    #
    # ```
    # require "thrift"
    #
    # class MyStruct
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
    #
    # my_struct = MyStruct.new(12, "hello")
    # my_struct.write to: protocol
    #
    # transport.peek # => Bytes[8, 0, 0, 0, 0, 0, 12, 11, 0, 1, 0, 0, 0, 5, 104, 101, 108, 108, 111, 0]
    # ```
    def write(to oprot : ::Thrift::Protocol::BaseProtocol)
      {% begin %}
      {%
          requires_write = @type.methods.select{|method| method.annotation(::Thrift::Type::SerialOpts) && method.annotation(::Thrift::Type::SerialOpts)[:requirement] == :required}
          opt_in_req_out_write = @type.methods.select{|method| method.annotation(::Thrift::Type::SerialOpts) && method.annotation(::Thrift::Type::SerialOpts)[:requirement] == :opt_in_req_out}
          optional_write = @type.methods.select{|method| method.annotation(::Thrift::Type::SerialOpts) && method.annotation(::Thrift::Type::SerialOpts)[:requirement] == :optional}
      %}

      oprot.write_recursion do
        begin
          oprot.write_struct_begin(self.class.name)

          {% if !opt_in_req_out_write.empty? %}
            # if opt_in_req_out fields are unset (required fields would need to be set to even be here)
            unless (%empty_fields = { {{opt_in_req_out_write.map{|write| "#{write.name.stringify} => !#{write.name.id}.nil?".id}.splat }} }.select{|k,v| !v}).empty?
              Log.for(self.class).error {"Required Field(s) missing during write: #{%empty_fields.keys.join(", ")}"}
            end
          {% end %}

          {% for write in requires_write %}
            oprot.write_field_begin({{write.annotation(::Thrift::Type::SerialOpts)[:transmit_name] || write.name.stringify}}, @{{write.name.id}}.thrift_type, {{write.annotation(::Thrift::Type::SerialOpts)[:fid].id}}_i16)
            @{{write.name.id}}.write to: oprot
            oprot.write_field_end
          {% end %}

          {% for write in opt_in_req_out_write %}
            @{{write.name.id}}.try do |{{write.name.id}}|
              oprot.write_field_begin({{write.annotation(::Thrift::Type::SerialOpts)[:transmit_name] || write.name.stringify}}, {{write.name.id}}.thrift_type, {{write.annotation(::Thrift::Type::SerialOpts)[:fid].id}}_i16)
              {{write.name.id}}.write to: oprot
              oprot.write_field_end
            end
          {% end %}

          {% for write in optional_write %}
            @__isset.{{write.name.id}} && @{{write.name.id}}.try do |{{write.name.id}}|
              oprot.write_field_begin({{write.annotation(::Thrift::Type::SerialOpts)[:transmit_name] || write.name.stringify}}, {{write.name.id}}.thrift_type, {{write.annotation(::Thrift::Type::SerialOpts)[:fid].id}}_i16)
              {{write.name.id}}.write to: oprot
              oprot.write_field_end
            end
          {% end %}
        ensure
          oprot.write_field_stop
          oprot.write_struct_end
        end
      end
      {% end %}
    end

    protected def read(from iprot : ::Thrift::Protocol::BaseProtocol)
      {% begin %}

      {% requires_check = @type.methods.select{|method| method.annotation(::Thrift::Type::SerialOpts) && method.annotation(::Thrift::Type::SerialOpts)[:requirement] == :required} %}
      iprot.read_recursion do
        {% if !requires_check.empty? %}
          %required_fields_set = {
            {{
              requires_check.map do |required|
                "#{required.name.stringify} => false".id
              end.splat
            }}
          }
        {% end %}
        iprot.read_struct_begin
        loop do
          fname, ftype, fid = iprot.read_field_begin
          break if ftype == ::Thrift::Types::Stop
          next if ftype == ::Thrift::Types::Void
          case {fid, ftype}
          {% for var in @type.methods.select(&.annotation(::Thrift::Type::SerialOpts)) %}
            {%
              type = if var.return_type == Nil
                raise "struct_property #{var.name} is a Nil"
              elsif var.return_type.is_a?(Union)
                var.return_type.types.find(&.!=(Nil))
              else
                var.return_type
              end
            %}
            when { {{var.annotation(::Thrift::Type::SerialOpts)[:fid].id}}, {{type}}.thrift_type }
              @{{var.name.id}} = {{type}}.read from: iprot
              {% if var.annotation(::Thrift::Type::SerialOpts)[:requirement] == :required %}
                %required_fields_set[{{var.name.stringify}}] = true
              {% elsif var.annotation(::Thrift::Type::SerialOpts)[:requirement] == :optional %}
                @__isset.{{var.name.id}} = true
              {% end %}
          {% end %}
          else
            iprot.skip ftype
          end
          iprot.read_field_end
        end
        iprot.read_struct_end
        {% if !requires_check.empty? %}
          %required_fields_set.select!{|k,v| !v}
          raise ::Thrift::Protocol::ProtocolException.new ::Thrift::Protocol::ProtocolException::INVALID_DATA, "Required field(s) not set when reading: #{%required_fields_set.keys.join(", ")}" unless %required_fields_set.empty?
        {% end %}
      end
      {% end %}
    end

    macro included
      {% verbatim do %}
        include Comparable(self)
        include ::Thrift::Type
        include ::Thrift::Type::Read
        extend ::Thrift::Type::ClassRead
        define_thrift_type ::Thrift::Types::Struct
        macro finished
          {% begin %}
          {% has_optionals = false %}
          {% for optional in @type.methods.select{|method| method.annotation(::Thrift::Type::SerialOpts) && method.annotation(::Thrift::Type::SerialOpts)[:requirement] == :optional} %}
            {% has_optionals = true %}
            struct {{@type.id}}__isset
              property {{optional.name.id}} : Bool = false
            end
          {% end %}
          {% if has_optionals %}
            @__isset = {{@type.id}}__isset.new
          {% end %}
          {% end %}
        end
      {% end %}
    end
  end
end