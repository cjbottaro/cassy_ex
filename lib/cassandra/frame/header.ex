defmodule Cassandra.Frame.Header do
  import Cassandra.Frame

  defstruct [
    version: :request,
    flags: 0x00,
    stream: 0x00,
    opcode: nil,
    length: 0
  ]

  def from_binary(data) do
    header = {%__MODULE__{}, data}
    |> byte(:version)
    |> byte(:flags)
    |> short(:stream)
    |> byte(:opcode)
    |> int(:length)
    |> elem(0)

    header = %{header |
      version: version(header.version),
      opcode: opcode(header.opcode)
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
