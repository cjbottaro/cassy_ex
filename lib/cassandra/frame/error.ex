defmodule Cassandra.Frame.Error do
  use Cassandra.Frame

  @opcode :error

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
