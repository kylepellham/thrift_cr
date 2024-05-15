require "./thrift_struct.cr"

module Thrift
  module Union

    annotation UnionVar
    end

    macro def_comp
      def <=>(other : self)
        if @storage__{{@type.name.id}}.class == other.@storage__{{@type.name.id}}.class
          return 0 if @storage__{{@type.name.id}}.nil?
          return @storage__{{@type.name.id}} <=> other.@storage__{{@type.name.id}}
        end
        return -1 if !@storage__{{@type.name.id}}.nil? && other.@storage__{{@type.name.id}}.nil?
        return 1 if @storage__{{@type.name.id}}.nil? && other.@storage__{{@type.name.id}}.nil?
        #really not much else we can compare at this point
        return @storage__{{@type.name.id}}.class.name <=> other.@storage__{{@type.name.id}}.class.name
      end
    end

    private macro generate_union_writer
      def write(oprot : ::Thrift::BaseProtocol, *args, **kwargs)
        validate
        oprot.write_struct_begin(\{{@type.stringify}})
        \{% begin %}
          case union_internal
          \{% for var in @type.methods.select(&.annotation(UnionVar)) %}
            when .is_a?(\{{var.return_type}})
              oprot.write_field_begin(\{{var.name.stringify}}, \{{var.name}}.thrift_type, \{{var.annotation(::Thrift::Struct::Property)[:id]}}.to_i16)
              \{{var.name}}.write(oprot)
              oprot.write_field_end
          \{% end %}
          end
        \{% end %}
        oprot.write_field_stop
        oprot.write_struct_end
      end
    end

    private macro generate_union_reader
      def self.read(iprot : ::Thrift::BaseProtocol)
        recieved_union = \{{@type}}.allocate
        iprot.read_struct_begin
        loop do
          name, ftype, fid = iprot.read_field_begin
          break if ftype == ::Thrift::Types::Stop
          raise "Too Many fields for Union" if union_set?
          \{% begin %}
          case {fid, ftype}
            \{% for var in @type.methods.select(&.annotation(UnionVar)) %}
              when  {\{{var.annotation(::Thrift::Struct::Property)[:id]}}, \{{var.return_type.id}}.thrift_type}
                recieved_struct.\{{var.name}} = \{{var.return_type}}.read from: iprot
            \{% end %}
          end
          \{% end %}
          iprot.read_field_end
        end
        iprot.read_struct_end
        recieved_union.validate
        return recieved_union
      end
    end


    macro included
      {% verbatim do %}
        include ::Thrift::Type
        define_thrift_type ::Thrift::Struct
        macro finished
          \{% begin %}
            \{%
              union_vars = @type.methods.select{ |method| method.annotation(UnionVar) }.map(&.return_type.id)
            %}
            @storage__{{@type.name.id}} : \{{ union_vars.join("|").id }} | Nil
          \{% end %}

          def initialize(**kwargs)
            if kwargs.size > 1
              raise ArgumentError.new "Expected 1 Argument #{kwargs.size} Given"
            end
            # default values are okay because at most only one of these conditions can be true
            keys = kwargs.keys
            \{% for var in @type.methods %}
              \{% if var.annotation(UnionVar) %}
                if keys.includes?(\{{var.name.symbolize}})
                  @storage = kwargs.fetch(\{{var.name.symbolize}}, nil.unsafe_as(\{{var.return_type.id}}))
                end
              \{% end %}
            \{% end %}
          end

          def ==(other : ::Thrift::Union)
            @storage__{{@type.name.id}} == other.@storage__{{@type.name.id}}
          end
        end
      {% end %}

      def_hash
      def_comp

      def union_set?
        !@storage__{{@type.name.id}}.nil?
      end

      def union_internal
        @storage__{{@type.name.id}}
      end
    end

    macro union_property(name)
      {% if name.is_a?(TypeDeclaration) %}
        {% if name.value %}
          {{raise "Unions Cannot Have Default Values"}}
        {% end %}
        @[UnionVar]
        def {{name.var.id}} : {{name.type.id}}
          return @storage__\{{@type.name.id}}.unsafe_as({{name.type.id}})
        end
        def {{name.var.id}}=({{name.var.id}}_val : {{name.type.id}})
          @storage__\{{@type.name.id}} = {{name.var.id}}_val
        end
        def is_{{name.var.id}}
          @storage__\{{@type.name.id}}.is_a?({{name.type.id}})
        end
      {% else %}
        {{ raise "Needs to be Type Declaration ex: union_property x : Int32" }}
      {% end %}
    end
  end
end
