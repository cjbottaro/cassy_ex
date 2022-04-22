defmodule Cassandra.Connection do
  use Connection
  alias Cassandra.Frame

  @default_opts [consistency: :quorum]

  def query(conn, cql, opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    Connection.call(conn, {:query, cql, opts})
  end

  def start_link(config \\ []) do
    {opts, config} = Keyword.split(config, [:name])
    Connection.start_link(__MODULE__, config, opts)
  end

  def init(config) do
    host = Keyword.get(config, :host, "localhost:9042")

    state = %{
      host: host,
      socket: nil,
      connected: false,
      stream: 0,
    }

    establish_connection(state)
  end

  def handle_call({:query, cql, opts}, _from, state) do
    {stream, state} = get_stream(state)

    frame = %Frame.Query{
      stream: stream,
      query: cql,
      consistency: opts[:consistency],
      values: opts[:values]
    }

    :ok = send_frame(state.socket, frame)

    {:reply, recv_frame(state.socket), state}
  end

  defp establish_connection(state) do
    [host, port] = String.split(state.host, ":")
    host = String.to_charlist(host)
    port = String.to_integer(port)
    opts = [:binary, active: false]

    with {:ok, socket} <- :gen_tcp.connect(host, port, opts),
      :ok <- send_frame(socket, %Frame.Startup{}),
      {:ok, %Frame.Ready{}} <- recv_frame(socket)
    do
      {:ok, %{state | socket: socket}}
    end
  end

  defp get_stream(state) do
    stream = state.stream
    {stream, %{state | stream: stream + 1}}
  end

  def send_frame(socket, frame) do
    data = Frame.to_iodata(frame)
    :gen_tcp.send(socket, data)
  end

  def recv_frame(socket) do
    with {:ok, header} <- :gen_tcp.recv(socket, 9),
      {:ok, header} <- Frame.Header.from_binary(header),
      {:ok, body} <- recv_body(socket, header.length)
    do
      Frame.from_binary(header, body)
    end
  end

  def recv_body(_socket, 0), do: {:ok, ""}
  def recv_body(socket, n), do: :gen_tcp.recv(socket, n)

end
