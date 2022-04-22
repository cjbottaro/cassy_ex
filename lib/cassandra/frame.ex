defmodule Cassandra.Frame do
  alias Cassandra.Frame

  defmacro __using__(_opts \\ []) do
    quote do
      import Cassandra.Frame.Data
      @before_compile Cassandra.Frame
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def opcode, do: @opcode
    end
  end

  def to_iodata(frame, header \\ []) do
    body = frame.__struct__.to_iodata(frame)

    header = header
    |> Keyword.put(:opcode, frame.__struct__.opcode())
    |> Keyword.put(:length, IO.iodata_length(body))

    header = struct!(Frame.Header, header)
    |> Frame.Header.to_iodata()

    [header, body]
  end

  def from_binary(header, data) do
    frame = case header.opcode do
      :ready -> Frame.Ready.from_binary(data)
      :error -> Frame.Error.from_binary(data)
      :result -> Frame.Result.from_binary(data)
    end

    {:ok, frame}
  end

end
