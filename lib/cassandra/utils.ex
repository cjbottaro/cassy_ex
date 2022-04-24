defmodule Cassandra.Utils do
  @moduledoc false

  # This isn't really a uuid, but it will serve our purposes.
  def uuid do
    <<a::4-bytes, b::2-bytes, c::2-bytes, d::2-bytes, e::6-bytes>> = :crypto.strong_rand_bytes(16)
    a = Base.encode16(a, case: :lower)
    b = Base.encode16(b, case: :lower)
    c = Base.encode16(c, case: :lower)
    d = Base.encode16(d, case: :lower)
    e = Base.encode16(e, case: :lower)
    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end

  defmacro if_test(do: block) do
    if Mix.env() == :test do
      quote do: unquote(block)
    end
  end

end
