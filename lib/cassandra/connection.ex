defmodule Cassandra.Connection do
  @moduledoc """
  Connect to a single Cassandra node

  """
  use Connection
  use Cassandra
  alias Elixir.Connection

  @type t :: GenServer.server()
  @type cql :: binary
  @type prepared :: %Result{kind: :prepared}

  @spec prepare(t, binary, Keyword.t) :: {:ok, Result.t} | {:error, Error.t}

  def prepare(conn, cql, opts \\ []) when is_binary(cql) do
    sha = :crypto.hash(:sha, cql)

    # Small race condition here between fetch_prepared and cache_prepared,
    # but it doesn't matter, at worst a query will be prepared twice.

    case Connection.call(conn, {:fetch_prepared, sha}) do
      nil ->
        if_test do: :telemetry.execute([:test, :fetch_prepared, :miss], %{})
        opts = Keyword.put(opts, :cql, cql)
        with {:ok, result} <- request(conn, Frame.Prepare, opts) do
          Connection.cast(conn, {:cache_prepared, sha, {result.query_id, result.columns}})
          {:ok, result}
        end

      {query_id, columns} ->
        if_test do: :telemetry.execute([:test, :fetch_prepared, :hit], %{})
        {:ok, %Result{kind: :prepared, query_id: query_id, columns: columns}}
    end
  end

  @spec prepare!(t, binary, Keyword.t) :: Result.t | no_return

  def prepare!(conn, cql, opts \\ []) when is_binary(cql) do
    case prepare(conn, cql, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec execute(t, cql | prepared, Keyword.t) :: {:ok, Result.t} | {:error, Error.t}

  def execute(conn, cql_or_prepared, opts \\ [])

  def execute(conn, cql, opts) when is_binary(cql) do
    opts = opts
    |> Keyword.put(:query, cql)
    |> Keyword.put_new(:consistency, :quorum)

    request(conn, Frame.Query, opts)
  end

  def execute(conn, %Result{kind: :prepared, query_id: query_id, columns: columns}, opts) when is_binary(query_id) do
    opts = opts
    |> Keyword.put(:query_id, query_id)
    |> Keyword.put(:columns, columns)
    |> Keyword.put_new(:consistency, :quorum)

    request(conn, Frame.Execute, opts)
  end

  @spec execute!(t, cql | prepared, Keyword.t) :: Result.t | no_return

  def execute!(conn, cql, opts \\ []) do
    case execute(conn, cql, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec stream!(t, cql | prepared, Keyword.t) :: Enumerable.t | no_return

  def stream!(conn, stmt, opts \\ []) do
    Stream.resource(
      fn -> execute!(conn, stmt, opts) end,

      fn
        %{kind: :rows, rows: [], paging_state: nil} ->
          {:halt, nil}

        %{kind: :rows, rows: rows, paging_state: nil} = result ->
          {rows, %{result | rows: []}}

        %{kind: :rows, rows: rows, paging_state: paging_state} ->
          {rows, execute!(conn, stmt, Keyword.put(opts, :paging_state, paging_state))}
      end,

      fn _ -> nil end
    )
  end

  defp request(conn, mod, opts) do
    {_time, stream} = :timer.tc fn -> Connection.call(conn, :next_stream) end

    with {:ok, frame} <- Frame.build(mod, stream, opts),
      {:ok, iodata} <- Frame.to_iodata(frame),
      {:ok, header, data} <- Connection.call(conn, {:send, stream, iodata}),
      {:ok, frame} <- Frame.from_binary(header, data)
    do
      case frame do
        %Frame.Result{} -> {:ok, Result.from_frame(frame)}
        %Frame.Error{} -> {:error, Error.from_frame(frame)}
      end
    else
      {:error, reason} -> {:error, Error.from_reason(reason)}
    end
  end

  @default_config [
    host: "localhost:9042",
    async_connect: false,
  ]

  def start(config \\ []) do
    with {:ok, config, opts} <- build_and_validate_config(config) do
      Connection.start(__MODULE__, config, opts)
    end
  end

  def start_link(config \\ []) do
    with {:ok, config, opts} <- build_and_validate_config(config) do
      Connection.start_link(__MODULE__, config, opts)
    end
  end

  def stop(conn) do
    GenServer.stop(conn)
  end

  def init(config) do
    state = %{
      host: config[:host],
      socket: nil,
      connected: false,
      stream: 0,
      waiters: %{},
      buffer: "",
      prepared: %{}
    }

    if config[:async_connect] do
      {:connect, :init, state}
    else
      establish_connection(state)
    end
  end

  def connect(_info, state) do
    establish_connection(state)
  end

  def handle_call(:next_stream, _from, %{stream: stream} = state) do
    {:reply, stream, %{state | stream: stream + 1}}
  end

  def handle_call(_args, _from, state) when not state.connected do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:send, stream, iodata}, from, state) do
    case :gen_tcp.send(state.socket, iodata) do
      :ok -> {:noreply, %{state | waiters: Map.put(state.waiters, stream, from)}}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:fetch_prepared, sha}, _from, state) when is_binary(sha) do
    {:reply, state.prepared[sha], state}
  end

  def handle_cast({:cache_prepared, sha, payload}, state) when is_binary(sha) do
    {:noreply, %{state | prepared: Map.put(state.prepared, sha, payload)}}
  end

  # Can this even happen?
  def handle_info({:tcp, _socket, data}, state) when byte_size(state.buffer) + byte_size(data) < 9 do
    {:noreply, %{state | buffer: state.buffer <> data}}
  end

  def handle_info({:tcp, socket, data}, state) do
    data = state.buffer <> data

    <<header::9-bytes, data::binary>> = data
    {:ok, header} = Frame.Header.from_binary(header)
    body_size = header.length
    data_size = byte_size(data)

    # Our packet could be less than a frame... or more than a frame?

    {body, buffer} = cond do
      body_size > data_size ->
        {:ok, more} = :gen_tcp.recv(socket, header.length - data_size)
        {data <> more, ""}

      body_size == data_size ->
        {data, ""}

      body_size < data_size ->
        <<data::binary-size(body_size), buffer::binary>> = data
        {data, buffer}
    end

    if_test do: :telemetry.execute([:test, :frame, :recv], %{})

    {from, waiters} = Map.pop!(state.waiters, header.stream)
    :ok = Connection.reply(from, {:ok, header, body})

    :ok = :inet.setopts(socket, [active: :once])

    {:noreply, %{state | waiters: waiters, buffer: buffer}}
  end

  defp establish_connection(state) do
    [host, port] = String.split(state.host, ":")
    host = String.to_charlist(host)
    port = String.to_integer(port)
    opts = [:binary, active: false]

    with {:ok, socket} <- :gen_tcp.connect(host, port, opts, 5_000), # TODO not hardcode
      {:ok, frame} <- Frame.build(Frame.Startup, 0),
      {:ok, iodata} <- Frame.to_iodata(frame),
      :ok <- :gen_tcp.send(socket, iodata),
      {:ok, header, data} <- recv_frame_data(socket),
      {:ok, %Frame.Ready{}} <- Frame.from_binary(header, data),
      :ok <- :inet.setopts(socket, [active: :once])
    do
      Logger.debug("Connection established to #{state.host}")
      {:ok, %{state | socket: socket, connected: true}}
    else
      {:ok, %Frame.Error{msg: msg}} ->
        Logger.error("Connection to #{state.host} failed: #{msg}")
        {:backoff, 2_000, state}

      {:error, reason} ->
        Logger.error("Connection to #{state.host} failed: #{reason}")
        {:backoff, 2_000, state}
    end
  end

  defp recv_frame_data(socket) do
    with {:ok, header} <- :gen_tcp.recv(socket, 9),
      {:ok, header} <- Frame.Header.from_binary(header),
      {:ok, body} <- recv_body(socket, header.length)
    do
      {:ok, header, body}
    end
  end

  defp recv_body(_socket, 0), do: {:ok, ""}
  defp recv_body(socket, n), do: :gen_tcp.recv(socket, n)

  defp build_and_validate_config(config) do
    {config, opts} = Keyword.merge(@default_config, config)
    |> Keyword.split([:host, :async_connect])

    with :ok <- validate_host(config[:host]) do
      {:ok, config, opts}
    end
  end

  defp validate_host(host) when is_binary(host) do
    host = String.trim(host)

    with [_host, port] <- String.split(host, ":") do
      try do
        _ = String.to_integer(port)
        :ok
      rescue
        ArgumentError -> {:error, "invalid port"}
      end
    else
      [""] -> {:error, "host is required"}
      [_] -> {:error, "port is missing from host"}
    end
  end
  defp validate_host(_), do: {:error, "host is required"}

end
