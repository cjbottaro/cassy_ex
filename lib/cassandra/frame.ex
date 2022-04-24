defmodule Cassandra.Frame do
  @moduledoc false

  use Cassandra

  @type t :: struct

  defmacro __using__(opts \\ []) do
    opcode = opts[:opcode]
    quote do
      import Cassandra.Frame.Data

      if unquote(opcode) do
        @opcode unquote(opcode)
      else
        @opcode Module.split(__MODULE__)
        |> List.last()
        |> Macro.underscore()
        |> String.to_atom()
      end

      def opcode, do: @opcode
    end
  end

  @spec build(module, non_neg_integer, Keyword.t, non_neg_integer) :: {:ok, t} | {:error, binary}

  def build(mod, stream, fields \\ [], flags \\ 0) do
    header = %Frame.Header{opcode: mod.opcode, stream: stream, flags: flags}
    try do
      {:ok, struct!(mod, Keyword.put(fields, :header, header))}
    rescue
      e in KeyError -> {:error, "invalid field :#{e.key}"}
    end
  end

  @spec to_iodata(t) :: {:ok, iolist} | {:error, Error.t}

  def to_iodata(frame) do
    body = frame.__struct__.to_iodata(frame)
    header = %{frame.header | length: IO.iodata_length(body)}
    |> Frame.Header.to_iodata()

    {:ok, [header, body]}
  rescue
    e in Frame.Data.Error -> {:error, e.message}
  end

  def from_binary(header, data) do
    frame = case header.opcode do
      :ready -> Frame.Ready.from_binary(data)
      :error -> Frame.Error.from_binary(data)
      :result -> Frame.Result.from_binary(data)
    end

    {:ok, %{frame | header: header}}
  end

end
