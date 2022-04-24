defmodule Cassandra.Frame.Execute do
  @moduledoc false
  use Cassandra.Frame

  defstruct [
    :header,
    :query_id,
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
    import Frame.Query, only: [query_params_iodata: 1]
    [
      short_bytes(frame.query_id),
      query_params_iodata(frame)
    ]
  end
end
