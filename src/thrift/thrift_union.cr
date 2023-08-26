module Thrift
  module Union
    annotation UnionVar
    end

    macro included
      {% verbatim do %}
        macro finished
          \{% begin %}
            \{%
              union_vars = @type.methods.select{ |method| method.annotation(UnionVar) }.map(&.return_type.id) 
            %}
            @storage : \{{ union_vars.join("|").id }} | Nil
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
        end
      {% end %}
    end

    macro union_property(name)
      {% if name.is_a?(TypeDeclaration) %}
        {% if name.value.symbolize != :"" %}
          {{raise "Unions Cannot Have Default Values"}}
        {% end %}
        @[UnionVar]
        def {{name.var.id}} : {{name.type.id}}
          return @storage.unsafe_as({{name.type.id}})
        end
        def {{name.var.id}}=({{name.var.id}}_val : {{name.type.id}})
          @storage = {{name.var.id}}_val
        end
      {% else %}
        {{ raise "Needs to be Type Declaration ex: union_property x : Int32" }}
      {% end %}
    end
  end
end