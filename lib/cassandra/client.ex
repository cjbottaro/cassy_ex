defmodule Cassandra.Client do
  use GenServer
  require Logger
  alias Cassandra.{Connection, Utils, Result, Error}

  @doc false
  def debug(client) do
    GenServer.call(client, :debug)
  end

  def execute(client, cql, opts \\ []) do
    GenServer.call(client, :conns)
    |> Enum.shuffle()
    |> Enum.reduce_while({:error, "no connections available"}, fn conn, _acc ->
      case Connection.execute(conn, cql, opts) do
        {:ok, %Result{}} = result -> {:halt, result}
        {:error, %Error{}} = error -> {:halt, error}
        error -> {:cont, error}
      end
    end)
  end

  @defaults [
    hosts: ["localhost:9042"],
    autodiscovery: true,
  ]

  def start_link(config \\ []) do
    config = Keyword.merge(@defaults, config)
    {opts, config} = Keyword.split(config, [:name])
    GenServer.start_link(__MODULE__, config, opts)
  end

  def init(config) do
    state = %{
      config: config,
      name: Utils.uuid,
      conns: [],
      pids: %{}
    }

    if config[:autodiscovery] do
      case autodiscovery(config, config[:hosts]) do
        {:ok, nodes} ->
          Logger.debug("Autodiscovered #{map_size(nodes)} node(s)")
          {:ok, connect_to_nodes(nodes, state)}

        {:error, reason} ->
          Logger.warn("Autodiscovery failed: #{reason}")
          Process.send_after(self(), :retry_autodiscovery, 2_000)
          {:ok, state}
      end
    else
      nodes = Map.new(config[:hosts], fn host -> {Utils.uuid, host} end)
      {:ok, connect_to_nodes(nodes, state)}
    end
  end

  def handle_call(:debug, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:conns, _from, state) do
    {:reply, state.conns, state}
  end

  def handle_info(:retry_autodiscovery, state) do
    %{config: config} = state

    case autodiscovery(config, config[:hosts]) do
      {:ok, nodes} ->
        Logger.debug("Autodiscovered #{map_size(nodes)} node(s)")
        {:noreply, connect_to_nodes(nodes, state)}

      {:error, reason} ->
        Logger.warn("Autodiscovery failed: #{reason}")
        Process.send_after(self(), :retry_autodiscovery, 2_000)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, _how}, state) when is_map_key(state.pids, pid) do
    {info, pids} = Map.pop!(state.pids, pid)

    Logger.warn("Connection to #{info.host} crashed, restarting...")

    config = state.config
    |> Keyword.put(:host, info.host)
    |> Keyword.put(:async_connect, true)
    |> Keyword.put(:name, info.conn)

    {:ok, pid} = Connection.start(config)
    pids = Map.put(pids, pid, info)
    Process.monitor(pid)

    {:noreply, %{state | pids: pids}}
  end

  @spec connect_to_nodes(map, map) :: map

  defp connect_to_nodes(nodes, state) do
    Enum.reduce(nodes, state, fn {host_id, address}, state ->
      config = Keyword.put(state.config, :host, address)
      |> Keyword.put(:name, {:global, {state.name, host_id}})

      {:ok, pid} = Connection.start(config)
      Process.monitor(pid)

      conn = {:global, {state.name, host_id}}
      conns = [conn | state.conns]
      pids = Map.put(state.pids, pid, %{
        conn: conn,
        host: address
      })

      %{state | conns: conns, pids: pids}
    end)
  end

  @spec autodiscovery(Keyword.t, [binary]) :: {:ok, map} | {:error, term}

  defp autodiscovery(_config, []) do
    {:error, "no hosts available"}
  end

  defp autodiscovery(config, [host | hosts]) do
    config = Keyword.put(config, :host, host)
    with {:ok, conn} <- Connection.start_link(config) do
      case autodiscovery(conn) |> tap(fn _ -> Connection.stop(conn) end) do
        {:ok, nodes} -> {:ok, nodes}
        _error -> autodiscovery(config, hosts)
      end
    end
  end

  @spec autodiscovery(Connection.t) :: {:ok, map} | {:error, term}

  defp autodiscovery(conn) do
    with {:ok, %{rows: [local]}} <- Connection.execute(conn, "SELECT host_id,rpc_address FROM system.local LIMIT 1"),
      {:ok, %{rows: peers}} <- Connection.execute(conn, "SELECT host_id,peer FROM system.peers")
    do
      nodes = [
        {local["host_id"], local["rpc_address"]} | Enum.map(peers, fn peer ->
          {peer["host_id"], peer["peer"]}
        end)
      ]
      |> Map.new(fn {host_id, address} ->
        address = Tuple.to_list(address)
        |> Enum.map(&to_string/1)
        |> Enum.join(".")

        {host_id, "#{address}:9042"} # TODO don't hardcode port
      end)

      {:ok, nodes}
    end
  end

end
