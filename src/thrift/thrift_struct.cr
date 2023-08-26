module Thrift
  module Struct
    macro included
      def vars
        {% for var in @type.instance_vars %}
          @\{{var.id}} = yield @\{{var.id}}, \{{var.stringify}}, \{{var.type}}
        {% end %}
      end
    end
  end
end