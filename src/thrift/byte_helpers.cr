struct Slice(T)
  def join(other : self)
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

  def << (other : self)
    join(other)
  end
end