defmodule Cassandra.Result do
  alias Cassandra.Frame

  @type t :: %__MODULE__{}

  defstruct [:kind, :rows, :count, :paging_state, :keyspace, :schema_change, :query_id, :columns]

  def from_frame(%Frame.Result{kind: :rows} = frame) do
    %__MODULE__{
      kind: :rows,
      rows: build_rows(frame),
      count: frame.row_count,
      paging_state: frame.paging_state,
    }
  end

  def from_frame(%Frame.Result{} = frame) do
    %__MODULE__{
      kind: frame.kind,
      keyspace: frame.keyspace,
      schema_change: frame.schema_change,
      query_id: frame.query_id,
      columns: frame.columns
    }
  end

  defp build_rows(frame) do
    columns = frame.columns

    Enum.reduce(frame.rows, [], fn row, acc ->
      {row, _i} = Enum.reduce(row, {%{}, 0}, fn value, {row, i} ->
        {_keyspace, _table, name, _type} = elem(columns, i)
        {Map.put(row, name, value), i+1}
      end)
      [row | acc]
    end)
  end

end
