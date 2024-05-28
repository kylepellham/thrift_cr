require "./thrift_struct.cr"

module Thrift
  module Union

    annotation UnionVar
    end

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

    def write(to oprot : ::Thrift::BaseProtocol)
      oprot.write_recursion do
        oprot.write_struct_begin({{@type.stringify}})
        {% begin %}
          case @storage
          {% for var in @type.methods.select(&.annotation(UnionVar)) %}
            when .is_a?({{var.return_type}})
              oprot.write_field_begin({{var.name.stringify}}, {{var.name}}.thrift_type, {{var.annotation(::Thrift::Type::Properties)[:fid]}}.to_i16)
              {{var.name}}.write(oprot)
              oprot.write_field_end
          {% end %}
          end
        {% end %}
        oprot.write_field_stop
        oprot.write_struct_end
      end
    end

    def read(from iprot : ::Thrift::BaseProtocol)
      iprot.read_recursion do
        iprot.read_struct_begin
        loop do
          name, ftype, fid = iprot.read_field_begin
          break if ftype == ::Thrift::Types::Stop
          raise "Too Many fields for Union" if union_set?
          {% begin %}
          case {fid, ftype}
            {% for var in @type.methods.select(&.annotation(UnionVar)) %}
              when  { {{var.annotation(::Thrift::Type::Properties)[:fid]}}, {{var.return_type.id}}.thrift_type }
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
