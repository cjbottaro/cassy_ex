defmodule Cassandra.Frame.Error do
  import Cassandra.Frame

  defstruct [:flags, :stream, :code, :msg]

  def from_binary(data) do
    {%__MODULE__{}, data}
    |> int(:code)
    |> string(:msg)
    |> elem(0)
  end
end
