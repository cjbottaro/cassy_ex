defmodule Cassandra.Frame.Data do
  @moduledoc false

  defmodule Error do
    @moduledoc false
    defexception [:message]
  end

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

  def short_bytes(b) do
    [short(byte_size(b)), b]
  end

  def long_string(s) do
    [int(byte_size(s)), s]
  end

  def string_list(l) do
    [
      short(length(l)),
      Enum.map(l, &string/1)
    ]
  end

  def string_map(map) do
    {i, data} = Enum.reduce(map, {0, []}, fn
      {_k, nil}, {i, data} -> {i, data}
      {k, v}, {i, data} -> {i+1, [[string(k), string(v)] | data]}
    end)

    [short(i), data]
  end

  @doc """
  Serialize [value]

  The argument _should_ be a tuple containing the type and value. If it isn't a
  tuple, the type will be guessed.

  ```
  iex> value({:text, "foo"})
  [<<0, 0, 0, 3>>, "foo"]

  iex> value("foo")
  [<<0, 0, 0, 3>>, "foo"]
  ```
  """
  def value({type, v}) do
    v = encode_value(type, v)
    [int(IO.iodata_length(v)), v]
  end

  def value(v) do
    v = encode_value(guess_type(v), v)
    [int(IO.iodata_length(v)), v]
  end

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

  def read_short_bytes(data) do
    {n, data} = read_short(data)
    <<s::binary-size(n), data::binary>> = data
    {s, data}
  end

  def read_string_map(data) do
    {i, data} = read_short(data)
    Enum.reduce(1..i, {%{}, data}, fn _, {map, data} ->
      {k, data} = read_string(data)
      {v, data} = read_string(data)
      map = Map.put(map, k, v)
      {map, data}
    end)
  end

  def read_string_list(data) do
    {n, data} = read_short(data)
    {l, data} = Enum.reduce(1..n, {[], data}, fn _, {l, data} ->
      {s, data} = read_string(data)
      {[s | l], data}
    end)

    {Enum.reverse(l), data}
  end

  def read_value(type, data) do
    {bytes, data} = read_bytes(data)
    {decode_value(type, bytes), data}
  end

  defp guess_type(nil), do: nil

  defp guess_type(n) when is_integer(n) and (n < -2_147_483_648 or n > 2_147_483_647) do
    :bigint
  end

  defp guess_type(n) when is_integer(n) and (n >= -2_147_483_648 and n <= 2_147_483_647) do
    :int
  end

  defp guess_type(<<_::8-bytes, ?-, _::4-bytes, ?-, _::4-bytes, ?-, _::4-bytes, ?-, _::12-bytes>>) do
    :uuid
  end

  defp guess_type(b) when is_binary(b) do
    :text
  end

  defp guess_type(m) when is_map(m) and map_size(m) == 0 do
    {:map, nil, nil}
  end

  defp guess_type(l) when is_list(l) do
    case l do
      [v | _] -> {:list, guess_type(v)}
      [] -> {:list, nil}
    end
  end

  defp guess_type(%MapSet{} = s) do
    if MapSet.size(s) == 0 do
      {:set, nil}
    else
      {:set, guess_type(Enum.at(s, 0))}
    end
  end

  defp guess_type(m) when is_map(m) and not is_struct(m) do
    if map_size(m) == 0 do
      {:map, nil, nil}
    else
      {k, v} = Enum.at(m, 0)
      {:map, guess_type(k), guess_type(v)}
    end
  end

  defp guess_type(t) when is_tuple(t) do
    {:tuple, Tuple.to_list(t) |> Enum.map(&guess_type/1)}
  end

  defp encode_value(:ascii, b) when is_binary(b), do: b

  defp encode_value(:bigint, n) when is_integer(n), do: <<n::integer-64>>

  defp encode_value(:blob, b) when is_binary(b), do: b

  defp encode_value(:boolean, true),  do: <<1::integer-8>>
  defp encode_value(:boolean, false), do: <<0::integer-8>>

  defp encode_value(:counter, n) when is_integer(n), do: <<n::integer-64>>

  defp encode_value(:date, %Date{} = d) do
    value = Date.diff(d, ~D[1970-01-01]) + 0x80000000
    <<value::integer-32>>
  end

  defp encode_value(:decimal, %Decimal{coef: coef, exp: exp, sign: sign}) do
    [encode_value(:int, -exp), encode_value(:varint, sign*coef)]
  end

  defp encode_value(:double, f) when is_float(f), do: <<f::float-64>>

  defp encode_value(:float, f) when is_float(f), do: <<f::float-32>>

  defp encode_value(:inet, {n1, n2, n3, n4}), do: <<n1, n2, n3, n4>>
  defp encode_value(:inet, {n1, n2, n3, n4, n5, n6, n7, n8}) do
    <<n1::16, n2::16, n3::16, n4::16, n5::16, n6::16, n7::16, n8::16>>
  end

  defp encode_value(:int, n), do: <<n::integer-32>>

  defp encode_value({:list, type}, l) do
    l = if is_list(l), do: l, else: Enum.into(l, [])
    [
      int(length(l)),
      Enum.map(l, fn v -> bytes(encode_value(type, v)) end)
    ]
  end

  defp encode_value({:map, ktype, vtype}, m) when is_map(m) do
    [
      int(map_size(m)),
      Enum.map(m, fn {k, v} ->
        k = encode_value(ktype, k)
        v = encode_value(vtype, v)
        [bytes(k), bytes(v)]
      end)
    ]
  end

  defp encode_value({:set, type}, s) do
    s = if is_struct(s, MapSet), do: s, else: Enum.into(s, MapSet.new)
    [
      int(MapSet.size(s)),
      Enum.map(s, fn v -> encode_value(type, v) |> bytes() end)
    ]
  end

  defp encode_value(:smallint, n) when is_integer(n), do: <<n::integer-16>>

  defp encode_value(:text, b) when is_binary(b), do: b

  defp encode_value(:time, %Time{} = t) do
    <<Time.diff(t, ~T[00:00:00.000000])::integer-64>>
  end

  defp encode_value(:timestamp, %DateTime{} = dt) do
    <<DateTime.to_unix(dt, :millisecond)::integer-64>>
  end

  defp encode_value(:timeuuid, b) when is_binary(b), do: encode_value(:uuid, b)

  defp encode_value(:tinyint, n) when is_integer(n), do: <<n::integer-8>>

  defp encode_value({:tuple, types}, t) do
    values = case t do
      t when is_tuple(t) -> Tuple.to_list(t)
      l when is_list(l) -> l
      e -> Enum.into(e, [])
    end

    Enum.zip(types, values)
    |> Enum.map(fn {type, value} ->
      bytes(encode_value(type, value))
    end)
  end

  defp encode_value(:uuid, b) when is_binary(b) do
    case byte_size(b) do
      16 -> b
      36 ->
        <<a::8-bytes, ?-, b::4-bytes, ?-, c::4-bytes, ?-, d::4-bytes, ?-, e::12-bytes>> = b
        <<
          Base.decode16!(a, case: :mixed)::4-bytes,
          Base.decode16!(b, case: :mixed)::2-bytes,
          Base.decode16!(c, case: :mixed)::2-bytes,
          Base.decode16!(d, case: :mixed)::2-bytes,
          Base.decode16!(e, case: :mixed)::6-bytes
        >>
    end
  end

  defp encode_value(:varchar, b) when is_binary(b), do: b

  defp encode_value(:varint, n) when is_integer(n) do
    size = varint_byte_size(n) * 8
    <<n::size(size)>>
  end

  # TODO catch this somehow and surface to user. Current this
  # crashes the connection and the client restarts it.
  defp encode_value(type, _value) do
    raise Error, "unrecognized CQL type '#{type}'"
  end

  defp varint_byte_size(value) when value > 127 do
    import Bitwise
    1 + varint_byte_size(value >>> 8)
  end

  defp varint_byte_size(value) when value < -128 do
    varint_byte_size(-value - 1)
  end

  defp varint_byte_size(_value), do: 1

  defp decode_value(_type, nil), do: nil

  defp decode_value(:boolean,  <<n::integer-8>>),         do: n != 0
  defp decode_value(:tinyint,  <<n::integer-signed-8>>),  do: n
  defp decode_value(:smallint, <<n::integer-signed-16>>), do: n
  defp decode_value(:int,      <<n::integer-signed-32>>), do: n
  defp decode_value(:float,    <<f::float-32>>),          do: f
  defp decode_value(:double,   <<f::float-64>>),          do: f
  defp decode_value(:bigint,   <<n::integer-signed-64>>), do: n
  defp decode_value(:counter,  <<n::integer-signed-64>>), do: n

  defp decode_value(type, data) when type in [:text, :ascii, :blob, :varchar], do: data

  defp decode_value({:list, type}, data) do
    {n, data} = read_int(data)
    {list, ""} = Enum.reduce(1..n, {[], data}, fn _, {list, data} ->
      {bytes, data} = read_bytes(data)
      value = decode_value(type, bytes)
      {[value | list], data}
    end)
    Enum.reverse(list)
  end

  defp decode_value({:map, ktype, vtype}, data) do
    {n, data} = read_int(data)
    {map, ""} = Enum.reduce(1..n, {%{}, data}, fn _, {map, data} ->
      {bytes, data} = read_bytes(data)
      k = decode_value(ktype, bytes)
      {bytes, data} = read_bytes(data)
      v = decode_value(vtype, bytes)
      {Map.put(map, k, v), data}
    end)
    map
  end

  defp decode_value({:set, type}, data) do
    {n, data} = read_int(data)
    {set, ""} = Enum.reduce(1..n, {MapSet.new(), data}, fn _, {set, data} ->
      {bytes, data} = read_bytes(data)
      value = decode_value(type, bytes)
      {MapSet.put(set, value), data}
    end)
    set
  end

  defp decode_value({:tuple, types}, data) do
    {acc, ""} = Enum.reduce(types, {[], data}, fn type, {acc, data} ->
      {bytes, data} = read_bytes(data)
      value = decode_value(type, bytes)
      {[value | acc], data}
    end)
    acc |> Enum.reverse() |> List.to_tuple()
  end

  defp decode_value(type, data) when type in [:uuid, :timeuuid] do
    <<a::4-bytes, b::2-bytes, c::2-bytes, d::2-bytes, e::6-bytes>> = data
    a = Base.encode16(a, case: :lower)
    b = Base.encode16(b, case: :lower)
    c = Base.encode16(c, case: :lower)
    d = Base.encode16(d, case: :lower)
    e = Base.encode16(e, case: :lower)
    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end

  defp decode_value(:timestamp, <<n::integer-64>>) do
    DateTime.from_unix!(n, :millisecond)
  end

  defp decode_value(:varint, data) do
    size = bit_size(data)
    <<value::size(size)-signed>> = data
    value
  end

  defp decode_value(:decimal, <<scale::32-signed, data::binary>>) do
    value = decode_value(data, :varint)
    sign = if value < 0, do: -1, else: 1
    Decimal.new(sign, abs(value), -scale)
  end

  defp decode_value(:inet, <<data::4-bytes>>) do
    <<n1, n2, n3, n4>> = data
    {n1, n2, n3, n4}
  end

  defp decode_value(:inet, <<data::16-bytes>>) do
    <<n1::16, n2::16, n3::16, n4::16, n5::16, n6::16, n7::16, n8::16>> = data
    {n1, n2, n3, n4, n5, n6, n7, n8}
  end

  defp decode_value(type, _data) do
    raise "CQL type '#{type}' not implemented for decoding yet"
  end

end
