defmodule Cassandra.Frame.Ready do
  use Cassandra.Frame

  @opcode :ready

  defstruct []

  def from_binary("") do
    %__MODULE__{}
  end
end
