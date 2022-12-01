defmodule Mix.Tasks.Benchmark do
  @shortdoc "Run benchmarks"

  @moduledoc """
  Run benchmarks again Xandra
  """
alias DBConnection.Connection

  use Mix.Task
  use Cassandra

  @requirements ["app.start"]
  @count 2000
  @concurrency 20
  @small_bytes 1024
  @large_bytes 100*1024

  @doc false
  def run(_args) do
    Logger.configure(level: :warn)

    label_filter = Jason.encode!(%{
      label: [
        "com.docker.compose.project=cassandra-ex",
        "com.docker.compose.service=node"
      ]
    })

    hosts = HTTPoison.get!("http+unix://%2Fvar%2Frun%2Fdocker.sock/containers/json", [],
      params: [filters: label_filter]
    )
    |> Map.get(:body)
    |> Jason.decode!()
    |> Enum.map(fn container ->
      port = Enum.find_value(container["Ports"], & &1["PublicPort"])
      || raise "Docker containers for Cassandra cluster not setup properly"

      "localhost:#{port}"
    end)

    host = Enum.random(hosts)

    {:ok, conn} = Connection.start_link(host: host)
    {:ok, xand} = Xandra.start_link(nodes: [host], pool_size: @concurrency)

    Connection.execute!(conn, """
      CREATE KEYSPACE IF NOT EXISTS test
      WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)

    Connection.execute!(conn, "DROP TABLE IF EXISTS test.benchmark")
    Connection.execute!(conn, """
      CREATE TABLE test.benchmark (
        id UUID PRIMARY KEY,
        b BLOB,
        m MAP<TEXT,TIMESTAMP>,
        s SET<INT>
      )
    """)

    ids = Enum.map(1..@count, fn _ -> uuid() end)

    # Warmup

    bench(Xandra, xand, :insert, ids, count: 100, bytes: 128, quiet: true)
    bench(Connection, conn, :insert, ids, count: 100, bytes: 128, quiet: true)

    bench(Xandra, xand, :select, ids, count: 100, quiet: true)
    bench(Connection, conn, :select, ids, count: 100, quiet: true)

    # Benchmarks

    IO.puts "INSERT (1kb rows)\n"
    bench(Xandra, xand, :insert, ids, bytes: @small_bytes)
    bench(Connection, conn, :insert, ids, bytes: @small_bytes)

    IO.puts "\nSELECT (1kb rows)\n"
    bench(Xandra, xand, :select, ids)
    bench(Connection, conn, :select, ids)

    IO.puts "\nINSERT (1kb rows, prepared)\n"
    bench(Xandra, xand, :insert, ids, bytes: @small_bytes, prepare: true)
    bench(Connection, conn, :insert, ids, bytes: @small_bytes, prepare: true)

    IO.puts "\nSELECT (1kb rows, prepared)\n"
    bench(Xandra, xand, :select, ids, prepare: true)
    bench(Connection, conn, :select, ids, prepare: true)

    IO.puts "\nINSERT (1mb rows)\n"
    bench(Xandra, xand, :insert, ids, bytes: @large_bytes)
    bench(Connection, conn, :insert, ids, bytes: @large_bytes)

    IO.puts "\nSELECT (1mb rows)\n"
    bench(Xandra, xand, :select, ids)
    bench(Connection, conn, :select, ids)

    IO.puts "\nINSERT (1mb rows, prepared)\n"
    bench(Xandra, xand, :insert, ids, bytes: @large_bytes, prepare: true)
    bench(Connection, conn, :insert, ids, bytes: @large_bytes, prepare: true)

    IO.puts "\nSELECT (1mb rows, prepared)\n"
    bench(Xandra, xand, :select, ids, prepare: true)
    bench(Connection, conn, :select, ids, prepare: true)

  end

  defp bench(mod, conn, type, ids, opts \\ []) do
    ids = case opts[:count] do
      nil -> ids
      n -> Enum.take(ids, n)
    end

    cql = case type do
      :insert -> """
        INSERT INTO test.benchmark (id, b, m, s)
        VALUES (?, ?, ?, ?)
      """

      :select -> """
        SELECT * FROM test.benchmark WHERE id = ?
      """
    end

    bytes = opts[:bytes]
    prepare = Keyword.get(opts, :prepare, false)
    concurrency = Keyword.get(opts, :concurrency, @concurrency)

    stmt = if prepare do
      mod.prepare!(conn, cql)
    else
      cql
    end

    {time, _} = :timer.tc fn ->

      Task.async_stream(ids, fn id ->
        params = params(mod, type, prepare, id, bytes)
        execute(mod, conn, stmt, params)
      end, max_concurrency: concurrency, ordered: false)
      |> Enum.each(fn {:ok, _} -> nil end)

    end

    if !opts[:quiet] do
      case mod do
        Xandra ->     IO.puts "  Xandra #{round(time/1000)}ms"
        Connection -> IO.puts "  Cassy  #{round(time/1000)}ms"
      end
    end
  end

  defp execute(Xandra, conn, stmt, params) do
    Xandra.execute!(conn, stmt, params, consistency: :quorum)
  end

  defp execute(Connection, conn, stmt, params) do
    Connection.execute!(conn, stmt, values: params, consistency: :quorum)
  end

  defp params(Xandra, :insert, false, id, bytes) do
    [
      {"uuid", id},
      {"blob", :rand.bytes(bytes)},
      {"map<text,timestamp>", %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
      {"set<int>", MapSet.new([1, 1, 2, 3])}
    ]
  end

  defp params(Connection, :insert, false, id, bytes) do
    [
      {:uuid, id},
      {:blob, :rand.bytes(bytes)},
      {{:map, :text, :timestamp}, %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
      {{:set, :int}, MapSet.new([1, 1, 2, 3])}
    ]
  end

  defp params(_mod, :insert, true, id, bytes) do
    [
      id,
      :rand.bytes(bytes),
      %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()},
      MapSet.new([1, 1, 2, 3])
    ]
  end

  defp params(Xandra, :select, false, id, nil) do
    [{"uuid", id}]
  end

  defp params(Connection, :select, false, id, nil) do
    [{:uuid, id}]
  end

  defp params(_mod, :select, true, id, nil) do
    [id]
  end

end
