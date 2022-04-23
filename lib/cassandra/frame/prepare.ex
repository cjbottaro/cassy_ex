defmodule Cassandra.Frame.Prepare do
  use Cassandra.Frame

  defstruct [:header, :cql]

  def to_iodata(%__MODULE__{} = frame) do
    long_string(frame.cql)
  end

end
