defmodule Cassandra.Frame.Query do
  @moduledoc false
  use Cassandra.Frame

  defstruct [
    :header,
    :query,
    :consistency,
    :no_metadata,
    :values,
    :page_size,
    :paging_state,
    :serial_consistency,
    :timestamp,
    :columns
  ]

  def to_iodata(frame) do
    [
      long_string(frame.query),
      query_params_iodata(frame)
    ]
  end

  def query_params_iodata(frame) do
    import Bitwise

    flags = 0
    iodata = []

    {flags, iodata} = case frame.values do
      # Positional bindings
      l when is_list(l) ->
        values = if frame.columns do
          Enum.zip(l, Enum.reverse(frame.columns)) # Remember columns are backwards.
          |> Enum.map(fn {v, column} ->
            {_keyspace, _table, _name, type} = column
            value({type, v})
          end)
        else
          Enum.map(l, &value/1)
        end
        {flags ||| 0x01, [iodata, short(length(l)), values]}

      # Named bindings
      m when is_map(m) ->
        values = if frame.columns do
          columns = Map.new(frame.columns, fn {_keyspace, _table, name, type} ->
            {name, type}
          end)
          Enum.map(m, fn {k, v} ->
            type = columns[to_string(k)]
            [string(k), value({type, v})]
          end)
        else
          Enum.map(m, fn {k, v} -> [string(k), value(v)] end)
        end
        {flags ||| 0x01 ||| 0x40, [iodata, short(map_size(m)), values]}

      # No bindings
      nil -> {flags, iodata}
    end

    {flags, iodata} = if frame.no_metadata do
      {flags ||| 0x02, iodata}
    else
      {flags, iodata}
    end

    {flags, iodata} = if frame.page_size do
      {flags ||| 0x04, [iodata, int(frame.page_size)]}
    else
      {flags, iodata}
    end

    {flags, iodata} = if frame.paging_state do
      {flags ||| 0x08, [iodata, bytes(frame.paging_state)]}
    else
      {flags, iodata}
    end

    {flags, iodata} = if frame.serial_consistency do
      {flags ||| 0x10, [iodata, consistency(frame.serial_consistency)]}
    else
      {flags, iodata}
    end

    {flags, iodata} = if frame.timestamp do
      {flags ||| 0x20, [iodata, long(frame.timestamp)]}
    else
      {flags, iodata}
    end

    [
      consistency(frame.consistency),
      byte(flags),
      iodata
    ]
  end

end
