#!/usr/bin/env ruby
#
# Red-black tree implementation.

class Node
  attr_accessor :red, :data, :rchild, :lchild

  def initialize(data = nil, red=false)
    @red = red
    @data = data
  end
end


