defmodule Cassandra.Frame.Result do
  use Cassandra.Frame

  @opcode :result

  defstruct [
    :header,
    :kind,
    :flags,
    :paging_state,
    :keyspace,
    :table,
    :columns,
    :row_count,
    :rows,
    :schema_change
  ]

  def from_binary(data) do
    {kind, data} = read_int(data)

    %__MODULE__{ kind: kind(kind) }
    |> from_binary(data)
  end

  defp from_binary(%{kind: :void} = frame, _data), do: frame

  defp from_binary(%{kind: :rows} = frame, data) do
    use Bitwise

    {flags, data} = read_int(data)
    {col_count, data} = read_int(data)

    {paging_state, data} = if (flags &&& 0x0002) != 0 do
      read_bytes(data)
    else
      {nil, data}
    end

    {keyspace, table, data} = if (flags &&& 0x0001) != 0 do
      {keyspace, data} = read_string(data)
      {table, data} = read_string(data)
      {keyspace, table, data}
    else
      {nil, nil, data}
    end

    {columns, data} = if (flags &&& 0x0001) == 0 do
      Enum.reduce(1..col_count, {[], data}, fn _, {columns, data} ->
        {keyspace, data} = read_string(data)
        {table, data} = read_string(data)
        {name, data} = read_string(data)
        {type, data} = read_option(data)

        {
          [{keyspace, table, name, type} | columns],
          data
        }
      end)
    else
      Enum.reduce(1..col_count, {[], data}, fn _, {columns, data} ->
        {name, data} = read_string(data)
        {type, data} = read_option(data)

        {
          [{keyspace, table, name, type} | columns],
          data
        }
      end)
    end

    {row_count, data} = read_int(data)

    {columns, rows} = if row_count > 0 do
      # Careful here... the columns are in reverse order due to using reduce,
      # hence why we count down from columns_count-1 to 0.

      columns = List.to_tuple(columns)
      {rows, _data} = Enum.reduce(0..row_count-1, {[], data}, fn _, {rows, data} ->
        {row, data} = Enum.reduce(col_count-1..0, {[], data}, fn i, {row, data} ->
          {_keyspace, _table, _name, type} = elem(columns, i)
          {value, data} = read_value(type, data)
          {[value | row], data}
        end)

        {[row | rows], data}
      end)

      {columns, rows}
    else
      {columns, []}
    end

    %{frame |
      flags: flags,
      paging_state: paging_state,
      keyspace: keyspace,
      table: table,
      columns: columns,
      row_count: row_count,
      rows: rows
    }
  end

  defp from_binary(%{kind: :set_keyspace} = frame, data) do
    {keyspace, _data} = read_string(data)
    %{frame | keyspace: keyspace}
  end

  defp from_binary(%{kind: :prepared} = _frame, _data) do
    raise "not implemented yet"
  end

  defp from_binary(%{kind: :schema_change} = frame, data) do
    {change_type, data} = read_string(data)
    {target, data} = read_string(data)
    {options, _data} = read_string(data)

    %{frame | schema_change: {change_type, target, options}}
  end

  def read_option(data) do
    {id, data} = read_short(data)

    case id do
      0x0000 -> read_string(data)
      0x0001 -> {:ascii, data}
      0x0002 -> {:bigint, data}
      0x0003 -> {:blob, data}
      0x0004 -> {:boolean, data}
      0x0005 -> {:counter, data}
      0x0006 -> {:decimal, data}
      0x0007 -> {:double, data}
      0x0008 -> {:float, data}
      0x0009 -> {:int, data}
      0x000b -> {:timestamp, data}
      0x000c -> {:uuid, data}
      0x000d -> {:varchar, data}
      0x000e -> {:varint, data}
      0x000f -> {:timeuuid, data}
      0x0010 -> {:inet, data}
      0x0011 -> {:date, data}
      0x0012 -> {:time, data}
      0x0013 -> {:smallint, data}
      0x0014 -> {:tinyint, data}
      0x0020 ->
        {type, data} = read_option(data)
        {{:list, type}, data}
      0x0021 ->
        {ktype, data} = read_option(data)
        {vtype, data} = read_option(data)
        {{:map, ktype, vtype}, data}
      0x0022 ->
        {type, data} = read_option(data)
        {{:set, type}, data}
      0x0031 ->
        {n, data} = read_short(data)
        {types, data} = Enum.reduce(1..n, {[], data}, fn _, {types, data} ->
          {type, data} = read_option(data)
          {[type | types], data}
        end)
        {{:tuple, Enum.reverse(types)}, data}
      _ ->
        id = Base.encode16(<<id::integer-16>>, case: :lower)
        raise "result option not implemented for 0x#{id}"
    end
  end

  [
    {:void,           0x0001},
    {:rows,           0x0002},
    {:set_keyspace,   0x0003},
    {:prepared,       0x0004},
    {:schema_change,  0x0005}
  ]
  |> Enum.each(fn {name, value} ->
    defp kind(unquote(value)), do: unquote(name)
  end)

end
