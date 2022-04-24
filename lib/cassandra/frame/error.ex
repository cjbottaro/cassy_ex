defmodule Cassandra.Frame.Error do
  @moduledoc false
  use Cassandra.Frame

  defstruct [:header, :code, :msg]

  def from_binary(data) do
    {code, data} = read_int(data)
    {msg, _data} = read_string(data)

    %__MODULE__{
      code: code,
      msg: msg
    }
  end
end
