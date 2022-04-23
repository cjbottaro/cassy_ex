defmodule Cassandra.Frame.Ready do
  use Cassandra.Frame

  defstruct [:header]

  def from_binary("") do
    %__MODULE__{}
  end
end
