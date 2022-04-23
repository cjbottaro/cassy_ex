defmodule Cassandra.Frame.Query do
  use Cassandra.Frame

  @opcode :query

  defstruct [
    :query,
    :consistency,
    :no_metadata,
    :values,
    :page_size,
    :paging_state,
    :serial_consistency,
    :timestamp,
  ]

  def to_iodata(frame) do
    use Bitwise

    flags = 0
    iodata = []

    {flags, iodata} = case frame.values do
      l when is_list(l) ->
        values = Enum.map(l, &value/1)
        {flags ||| 0x01, [iodata, short(length(l)), values]}

      m when is_map(m) ->
        values = Enum.map(m, fn {k, v} ->
          [string(k), value(v)]
        end)
        {flags ||| 0x01 ||| 0x40, [iodata, short(map_size(m)), values]}

      nil -> {flags, iodata}
    end

    {flags, iodata} = if frame.no_metadata do
      {flags ||| 0x02, iodata}
    else
      {flags, iodata}
    end

    {flags, iodata} = if frame.page_size do
      {flags ||| 0x04, [iodata, int(frame.page_size)]}
      |> IO.inspect
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
      long_string(frame.query),
      consistency(frame.consistency),
      byte(flags),
      iodata
    ]
  end

end
