defmodule Cassandra.Frame.Startup do
  use Cassandra.Frame

  @opcode :startup

  defstruct [
    header: nil,
    compression: nil,
    no_compact: nil,
    throw_on_overload: nil
  ]

  def to_iodata(frame) do
    string_map(
      CQL_VERSION: "3.0.0",
      COMPRESSION: frame.compression,
      NO_COMPACT: frame.no_compact,
      THROW_ON_OVERLOAD: frame.throw_on_overload
    )
  end


end
