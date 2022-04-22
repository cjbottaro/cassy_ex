defmodule Cassandra.Frame.Data do

  [
    {:request,  0x04},
    {:response, 0x84}
  ]
  |> Enum.each(fn {name, value} ->
    def version(unquote(name)), do: unquote(value)
    def version(unquote(value)), do: unquote(name)
  end)

  [
    {:error,          0x00},
    {:startup,        0x01},
    {:ready,          0x02},
    {:authenticate,   0x03},
    {:options,        0x05},
    {:supported,      0x06},
    {:query,          0x07},
    {:result,         0x08},
    {:prepare,        0x09},
    {:execute,        0x0A},
    {:register,       0x0B},
    {:event,          0x0C},
    {:batch,          0x0D},
    {:auth_challenge, 0x0E},
    {:auth_response,  0x0F},
    {:auth_success,   0x10},
  ]
  |> Enum.each(fn {name, value} ->
    def opcode(unquote(name)), do: unquote(value)
    def opcode(unquote(value)), do: unquote(name)
  end)

  [
    {:any,          0x0000},
    {:one,          0x0001},
    {:two,          0x0002},
    {:three,        0x0003},
    {:quorum,       0x0004},
    {:all,          0x0005},
    {:local_quorum, 0x0006},
    {:each_quorum,  0x0007},
    {:serial,       0x0008},
    {:local_serial, 0x0009},
    {:local_one,    0x000A},
  ]
  |> Enum.each(fn {name, value} ->
    def consistency(unquote(name)), do: short(unquote(value))
    def consistency(unquote(value)), do: unquote(name)
  end)

  def read_byte(data) do
    <<n::integer-8, data::binary>> = data
    {n, data}
  end

  def read_short(data) do
    <<n::integer-16, data::binary>> = data
    {n, data}
  end

  def read_int(data) do
    <<n::integer-signed-32, data::binary>> = data
    {n, data}
  end

  def read_long(data) do
    <<n::integer-signed-64, data::binary>> = data
    {n, data}
  end

  def read_string(data) do
    {n, data} = read_short(data)
    <<s::binary-size(n), data::binary>> = data
    {s, data}
  end

  def read_bytes(data) do
    {n, data} = read_int(data)
    if n < 0 do
      {nil, data}
    else
      <<s::binary-size(n), data::binary>> = data
      {s, data}
    end
  end

  def read_value(data, type) do
    {bytes, data} = read_bytes(data)
    {decode_value(type, bytes), data}
  end

  def byte(n) do
    <<n::integer-8>>
  end

  def short(n) do
    <<n::integer-16>>
  end

  def int(n) do
    <<n::integer-32>>
  end

  def long(n) do
    <<n::integer-64>>
  end

  def string(s) when is_binary(s) do
    [short(byte_size(s)), s]
  end

  def string(a) when is_atom(a) do
    Atom.to_string(a) |> string()
  end

  def bytes(nil) do
    int(-1)
  end

  def bytes(b) do
    [int(byte_size(b)), b]
  end

  def long_string(s) do
    [int(byte_size(s)), s]
  end

  def string_map(map) do
    {i, data} = Enum.reduce(map, {0, []}, fn
      {_k, nil}, {i, data} -> {i, data}
      {k, v}, {i, data} -> {i+1, [[string(k), string(v)] | data]}
    end)

    [short(i), data]
  end

  def value(n) when is_integer(n) do
    [int(4), <<n::integer-32>>]
  end

  def decode_value(_type, nil), do: nil

  def decode_value(:boolean,  <<n::integer-8>>),         do: n != 0
  def decode_value(:tinyint,  <<n::integer-signed-8>>),  do: n
  def decode_value(:smallint, <<n::integer-signed-16>>), do: n
  def decode_value(:int,      <<n::integer-signed-32>>), do: n
  def decode_value(:float,    <<f::float-32>>),          do: f
  def decode_value(:double,   <<f::float-64>>),          do: f
  def decode_value(:bigint,   <<n::integer-signed-64>>), do: n
  def decode_value(:counter,  <<n::integer-signed-64>>), do: n

  def decode_value(type, data) when type in [:text, :ascii, :blob, :varchar], do: data

  def decode_value(type, data) when type in [:uuid, :timeuuid] do
    <<a::4-bytes, b::2-bytes, c::2-bytes, d::2-bytes, e::6-bytes>> = data
    a = Base.encode16(a, case: :lower)
    b = Base.encode16(b, case: :lower)
    c = Base.encode16(c, case: :lower)
    d = Base.encode16(d, case: :lower)
    e = Base.encode16(e, case: :lower)
    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end

  def decode_value(:timestamp, <<n::integer-64>>) do
    DateTime.from_unix!(n, :millisecond)
  end

  def decode_value(:varint, data) do
    size = bit_size(data)
    <<value::size(size)-signed>> = data
    value
  end

  def decode_value(:decimal, <<scale::32-signed, data::binary>>) do
    value = decode_value(data, :varint)
    sign = if value < 0, do: -1, else: 1
    Decimal.new(sign, abs(value), -scale)
  end

  def decode_value(:inet, <<data::4-bytes>>) do
    <<n1, n2, n3, n4>> = data
    {n1, n2, n3, n4}
  end

  def decode_value(:inet, <<data::16-bytes>>) do
    <<n1::16, n2::16, n3::16, n4::16, n5::16, n6::16, n7::16, n8::16>> = data
    {n1, n2, n3, n4, n5, n6, n7, n8}
  end

  def decode_value(type, _data) do
    raise "CQL type '#{type}' not implemented for decoding yet"
  end

end
