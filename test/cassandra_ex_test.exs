defmodule CassandraExTest do
  use ExUnit.Case
  doctest CassandraEx

  test "greets the world" do
    assert CassandraEx.hello() == :world
  end
end
