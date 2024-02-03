require "./types.cr"
require "./protocol/base_protocol.cr"

# reopen slice so we can make a succinct way to expand slices
struct Slice(T)
  def join_with(other : self)
    new_slice = Pointer(T).malloc self.size + other.size
    appender = new_slice.appender
    self.each do |element|
      appender << element
    end
    other.each do |element|
      appender << element
    end
    Slice.new(new_slice, appender.size)
  end

  def <<(other : self)
    join_with(other)
  end
end