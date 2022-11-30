defmodule Mix.Tasks.Benchmark do
  @shortdoc "Run benchmarks"

  @moduledoc """
  Run benchmarks again Xandra
  """

  use Mix.Task
  use Cassandra

  @requirements ["app.start"]

  @doc false
  def run(_args) do
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
    {:ok, xand} = Xandra.start_link(nodes: [host])

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

    ids = Enum.map(1..1000, fn _ -> uuid() end)

    # Warmup
    Enum.take(ids, 10)
    |> Enum.each(fn id ->
      Xandra.execute!(xand, """
        INSERT INTO test.benchmark (id, b, m, s)
        VALUES (?, ?, ?, ?)
      """,
        [
          {"uuid", id},
          {"blob", :rand.bytes(1024)},
          {"map<text,timestamp>", %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
          {"set<int>", MapSet.new([1, 1, 2, 3])}
        ]
      )
    end)

    IO.puts "INSERT..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Xandra.execute!(xand, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """,
          [
            {"uuid", id},
            {"blob", :rand.bytes(1024)},
            {"map<text,timestamp>", %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
            {"set<int>", MapSet.new([1, 1, 2, 3])}
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Connection.execute!(conn, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """,
          values: [
            {:uuid, id},
            {:blob, :rand.bytes(1024)},
            {{:map, :text, :timestamp}, %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
            {{:set, :int}, MapSet.new([1, 1, 2, 3])}
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    IO.puts "\nSELECT..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Xandra.execute!(xand, "SELECT * FROM test.benchmark WHERE id = ?", [
          {"uuid", id}
        ], consistency: :quorum)
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Connection.execute!(conn, "SELECT * FROM test.benchmark WHERE id = ?",
          values: [
            {:uuid, id}
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    IO.puts "\nINSERT (prepared)..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Xandra.prepare!(xand, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """)
        Xandra.execute!(xand, stmt,
          [
            id,
            :rand.bytes(1024),
            %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()},
            MapSet.new([1, 1, 2, 3])
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Connection.prepare!(conn, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """)
        Connection.execute!(conn, stmt,
          values: [
            id,
            :rand.bytes(1024),
            %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()},
            MapSet.new([1, 1, 2, 3])
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    IO.puts "\nSELECT (prepared)..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Xandra.prepare!(xand, "SELECT * FROM test.benchmark WHERE id = ?")
        Xandra.execute!(xand, stmt, [id], consistency: :quorum)
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Connection.prepare!(conn, "SELECT * FROM test.benchmark WHERE id = ?")
        Connection.execute!(conn, stmt, values: [id], consistency: :quorum)
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    num_bytes = 1024*1024

    IO.puts "\nINSERT (large)..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Xandra.execute!(xand, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """,
          [
            {"uuid", id},
            {"blob", :rand.bytes(num_bytes)},
            {"map<text,timestamp>", %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
            {"set<int>", MapSet.new([1, 1, 2, 3])}
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        Connection.execute!(conn, """
          INSERT INTO test.benchmark (id, b, m, s)
          VALUES (?, ?, ?, ?)
        """,
          values: [
            {:uuid, id},
            {:blob, :rand.bytes(num_bytes)},
            {{:map, :text, :timestamp}, %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
            {{:set, :int}, MapSet.new([1, 1, 2, 3])}
          ],
          consistency: :quorum
        )
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    IO.puts "\nSELECT (large)..."

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Xandra.prepare!(xand, "SELECT * FROM test.benchmark WHERE id = ?")
        Xandra.execute!(xand, stmt, [id], consistency: :quorum)
      end)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Enum.each(ids, fn id ->
        stmt = Connection.prepare!(conn, "SELECT * FROM test.benchmark WHERE id = ?")
        Connection.execute!(conn, stmt, values: [id], consistency: :quorum)
      end)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    num_bytes = 8*1024*1024

    IO.puts "\nINSERT (single large)..."

    {time, _} = :timer.tc fn ->
      Xandra.execute!(xand, """
        INSERT INTO test.benchmark (id, b, m, s)
        VALUES (?, ?, ?, ?)
      """,
        [
          {"uuid", Enum.at(ids, 0)},
          {"blob", :rand.bytes(num_bytes)},
          {"map<text,timestamp>", %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
          {"set<int>", MapSet.new([1, 1, 2, 3])}
        ],
        consistency: :quorum
      )
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      Connection.execute!(conn, """
        INSERT INTO test.benchmark (id, b, m, s)
        VALUES (?, ?, ?, ?)
      """,
        values: [
          {:uuid, Enum.at(ids, 0)},
          {:blob, :rand.bytes(num_bytes)},
          {{:map, :text, :timestamp}, %{"foo" => DateTime.utc_now(), "bar" => DateTime.utc_now()}},
          {{:set, :int}, MapSet.new([1, 1, 2, 3])}
        ],
        consistency: :quorum
      )
    end

    IO.puts "Cassy  #{round(time/1000)}ms"

    IO.puts "\nSELECT (large)..."

    {time, _} = :timer.tc fn ->
      stmt = Xandra.prepare!(xand, "SELECT * FROM test.benchmark WHERE id = ?")
      Xandra.execute!(xand, stmt, [Enum.at(ids, 0)], consistency: :quorum)
    end

    IO.puts "Xandra #{round(time/1000)}ms"

    {time, _} = :timer.tc fn ->
      stmt = Connection.prepare!(conn, "SELECT * FROM test.benchmark WHERE id = ?")
      Connection.execute!(conn, stmt, values: [Enum.at(ids, 0)], consistency: :quorum)
    end

    IO.puts "Cassy  #{round(time/1000)}ms"
  end

end
