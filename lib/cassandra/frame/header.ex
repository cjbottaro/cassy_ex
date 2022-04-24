defmodule Cassandra.Frame.Header do
  @moduledoc false
  import Cassandra.Frame.Data

  defstruct [
    version: :request,
    flags: 0x00,
    stream: 0x00,
    opcode: nil,
    length: 0
  ]

  def from_binary(data) do
    {version, data} = read_byte(data)
    {flags,   data} = read_byte(data)
    {stream,  data} = read_short(data)
    {opcode,  data} = read_byte(data)
    {length, _data} = read_int(data)

    header = %__MODULE__{
      version: version(version),
      opcode: opcode(opcode),
      flags: flags,
      stream: stream,
      length: length
    }

    {:ok, header}
  end

  def to_iodata(header) do
    version = version(header.version)
    opcode = opcode(header.opcode)

    [
      byte(version),
      byte(header.flags),
      short(header.stream),
      byte(opcode),
      int(header.length)
    ]
  end
end
