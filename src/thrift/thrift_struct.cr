module Thrift
  module Struct

    def write(oprot)
      raise NotImplementedException.new
    end

    # macro included

    #   property field_info = {
    #     {% for var in @type.instance_vars %}
    #       \{{var.name.upcase}} => \{{var.type.id}},
    #     {% end %}

    #   }

    #   def vars
    #     {% for var in @type.instance_vars %}
    #       @\{{var.id}} = yield \{{var.name.upcase}}, \{{var.stringify}}, \{{var.type.name}}
    #     {% end %}
    #   end

    #   def write_impl(oprot, value, type)      
    #   end

    #   def write(oprot)
    #     validate
    #     oprot.write_struct_begin(\{{@type.name}})
    #     # vars do |
    #   end
    # end
    # {% debug %}
  end
end