defmodule Cassandra.Frame.Execute do
  use Cassandra.Frame

  @opcode :execute

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
  ]

  def to_iodata(frame) do
    [
      short_bytes(frame.query_id),
      Cassandra.Frame.Query.query_params_iodata(frame)
    ]
  end
end
