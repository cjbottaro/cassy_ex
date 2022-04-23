defmodule Cassandra.Connection do
  use Connection
  require Logger
  alias Cassandra.{Frame, Result, Error}

  @type t :: GenServer.server()

  def execute(conn, cql, opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :quorum)

    frame = %Frame.Query{
      query: cql,
      consistency: consistency,
      values: opts[:values]
    }

    with {:ok, header, data} <- Connection.call(conn, {:query, frame}),
      {:ok, frame} <- Frame.from_binary(header, data)
    do
      case frame do
        %Frame.Result{} -> {:ok, Result.from_frame(frame)}
        %Frame.Error{} -> {:error, Error.from_frame(frame)}
      end
    end
  end

  def start(config \\ []) do
    {opts, config} = Keyword.split(config, [:name])
    Connection.start(__MODULE__, config, opts)
  end

  def start_link(config \\ []) do
    {opts, config} = Keyword.split(config, [:name])
    Connection.start_link(__MODULE__, config, opts)
  end

  def stop(conn) do
    GenServer.stop(conn)
  end

  def init(config) do
    host = Keyword.get(config, :host, "localhost:9042")

    state = %{
      host: host,
      socket: nil,
      connected: false,
      stream: 0,
      waiters: %{},
      buffer: ""
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

  def handle_call(_args, _from, state) when not state.connected do
    {:reply, {:error, :not_connected}, state}
  end

  def handle_call({:query, frame}, from, state) do
    {stream, state} = get_stream(state)

    case send_frame(state.socket, frame, stream: stream) do
      :ok -> {:noreply, %{state | waiters: Map.put(state.waiters, stream, from)}}
      error -> {:reply, error, state}
    end
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
      :ok <- send_frame(socket, %Frame.Startup{}),
      {:ok, header, data} <- recv_frame(socket),
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

  defp get_stream(state) do
    stream = state.stream
    {stream, %{state | stream: stream + 1}}
  end

  defp send_frame(socket, frame, header \\ []) do
    data = Frame.to_iodata(frame, header)
    :gen_tcp.send(socket, data)
  end

  defp recv_frame(socket) do
    with {:ok, header} <- :gen_tcp.recv(socket, 9),
      {:ok, header} <- Frame.Header.from_binary(header),
      {:ok, body} <- recv_body(socket, header.length)
    do
      {:ok, header, body}
    end
  end

  def recv_body(_socket, 0), do: {:ok, ""}
  def recv_body(socket, n), do: :gen_tcp.recv(socket, n)

end
