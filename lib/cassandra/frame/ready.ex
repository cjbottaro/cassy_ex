defmodule Cassandra.Frame.Ready do
  defstruct [:flags, :stream]

  def from_binary("") do
    %__MODULE__{}
  end
end
