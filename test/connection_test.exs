defmodule CassandraTest do
  use ExUnit.Case
  use Cassandra
  doctest Connection

  setup_all do

    label_filter =Jason.encode!(%{
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

    {:ok, conn} = Connection.start_link(host: Enum.random(hosts))
    {:ok, xand} = Xandra.start_link(nodes: [Enum.random(hosts)])

    {:ok, _} = Connection.execute(conn, """
      CREATE KEYSPACE IF NOT EXISTS test
      WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)
    {:ok, _} = Connection.execute(conn, """
      CREATE TABLE IF NOT EXISTS test.collections (
        id UUID PRIMARY KEY,
        m MAP<int,text>,
        s set<int>,
        l list<text>,
        t tuple<int,text,uuid>
      )
    """)

    Connection.execute!(conn, "DROP TABLE IF EXISTS test.prepared")
    Connection.execute!(conn, """
      CREATE TABLE test.prepared (
        id UUID PRIMARY KEY,
        n SMALLINT,
        s SET<SMALLINT>
      )
    """)

    Connection.execute!(conn, "DROP TABLE IF EXISTS test.paging")
    Connection.execute!(conn, """
      CREATE TABLE test.paging (
        id UUID,
        n INT,
        PRIMARY KEY (id, n)
      )
    """)

    Connection.execute!(conn, "DROP TABLE IF EXISTS test.large")
    Connection.execute!(conn, """
      CREATE TABLE test.large (
        id UUID PRIMARY KEY,
        b BLOB
      )
    """)

    %{conn: conn, xand: xand}
  end

  test "basic query", %{conn: conn} do
    {:ok, result} = Connection.execute(conn, "select * from system.peers")
    assert is_struct(result, Result)
    assert result.kind == :rows
    assert result.count == 2
    assert length(result.rows) == result.count
  end

  describe "collections" do

    test "literals", %{conn: conn} do
      id = uuid()

      {:ok, %Result{}} = Connection.execute(conn, """
        insert into test.collections (id, m, s, l, t) values (
          #{id},
          { 1 : 'one' },
          {1, 2, 3, 3},
          ['one', 'one', 'two'],
          (1, 'one', ab9dcec9-2877-46d9-9e63-be00a94ac900)
        )
      """)

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = #{id}"
      )

      assert row["id"] == id
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

    test "positional", %{conn: conn} do
      id = uuid()

      {:ok, %Result{}} = Connection.execute(conn,
        "insert into test.collections (id, m, s, l, t) values (?, ?, ?, ?, ?)",
        values: [
          id,
          %{1 => "one"},
          MapSet.new([1, 2, 3, 3]),
          ["one", "one", "two"],
          {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
        ]
      )

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = #{id}"
      )

      assert row["id"] == id
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

    test "named", %{conn: conn} do
      id = uuid()

      {:ok, %Result{}} = Connection.execute(conn,
        "insert into test.collections (id, m, s, l, t) values (:id, :m, :s, :l, :t)",
        values: %{
          id: id,
          m: %{1 => "one"},
          s: MapSet.new([1, 2, 3, 3]),
          l: ["one", "one", "two"],
          t: {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
        }
      )

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = #{id}"
      )

      assert row["id"] == id
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

  end

  describe "prepared" do

    test "caching", %{conn: conn} do
      id = uuid()

      test = fn stmt ->
        %{kind: :void} = Connection.execute!(conn, stmt,
          values: [
            id,
            %{1 => "one"},
            MapSet.new([1, 2, 3, 3]),
            ["one", "one", "two"],
            {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
          ]
        )

        %Result{rows: [row]} = Connection.execute!(conn,
          "select * from test.collections where id = #{id}"
        )

        assert row["id"] == id
        assert row["m"] == %{ 1 => "one" }
        assert row["s"] == MapSet.new([1, 2, 3])
        assert row["l"] == ["one", "one", "two"]
        assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
      end

      {miss_count, stmt} = count_events([:test, :fetch_prepared, :miss], fn ->
        Connection.prepare!(conn,
          "insert into test.collections (id, m, s, l, t) values (?, ?, ?, ?, ?)"
        )
      end)

      assert miss_count == 1
      test.(stmt)

      {hit_count, stmt} = count_events([:test, :fetch_prepared, :hit], fn ->
        Connection.prepare!(conn,
          "insert into test.collections (id, m, s, l, t) values (?, ?, ?, ?, ?)"
        )
      end)

      assert hit_count == 1
      test.(stmt)
    end

    test "doesn't need type inference (positional)", %{conn: conn} do
      id = uuid()

      {:error, %{type: :invalid}} = Connection.execute(conn,
        "insert into test.prepared (id, n, s) values (?, ?, ?)",
        values: [id, 123, [1, 1, 2]]
      )

      stmt = Connection.prepare!(conn, "insert into test.prepared (id, n, s) values (?, ?, ?)")
      Connection.execute!(conn, stmt, values: [id, 123, [1, 1, 2]])

      %{rows: [row]} = Connection.execute!(conn, "select * from test.prepared where id = #{id}")
      assert row == %{"id" => id, "n" => 123, "s" => MapSet.new([1, 2])}
    end

    test "doesn't need type inference (named)", %{conn: conn} do
      id = uuid()

      {:error, %{type: :invalid}} = Connection.execute(conn,
        "insert into test.prepared (id, n, s) values (:id, :n, :s)",
        values: %{id: id, n: 123, s: [1, 1, 2]}
      )

      stmt = Connection.prepare!(conn, "insert into test.prepared (id, n, s) values (:id, :n, :s)")
      Connection.execute!(conn, stmt, values: %{id: id, n: 123, s: [1, 1, 2]})

      %{rows: [row]} = Connection.execute!(conn, "select * from test.prepared where id = #{id}")
      assert row == %{"id" => id, "n" => 123, "s" => MapSet.new([1, 2])}
    end

  end

  describe "paging" do

    test "unprepared", %{conn: conn} do
      id = uuid()

      Enum.each(1..10, fn n ->
        Connection.execute!(conn, "insert into test.paging (id, n) values (#{id}, #{n})")
      end)

      {count1, values} = count_events([:test, :frame, :recv], fn ->
        Connection.stream!(conn, "select n from test.paging where id = #{id}", page_size: 2)
        |> Enum.map(& &1["n"])
      end)

      assert values == Enum.into(1..10, [])
      assert count1 >= 5

      {count2, values} = count_events([:test, :frame, :recv], fn ->
        Connection.stream!(conn, "select n from test.paging where id = #{id}", page_size: 2)
        |> Enum.take(5)
        |> Enum.map(& &1["n"])
      end)

      assert values == Enum.into(1..5, [])
      assert count2 >= 3

      assert count1 > count2
    end

    test "prepared", %{conn: conn} do
      id = uuid()

      Enum.each(1..10, fn n ->
        Connection.execute!(conn, "insert into test.paging (id, n) values (#{id}, #{n})")
      end)

      stmt = Connection.prepare!(conn, "select n from test.paging where id = ?")

      {count1, values} = count_events([:test, :frame, :recv], fn ->
        Connection.stream!(conn, stmt, values: [id], page_size: 2)
        |> Enum.map(& &1["n"])
      end)

      assert values == Enum.into(1..10, [])
      assert count1 >= 5

      {count2, values} = count_events([:test, :frame, :recv], fn ->
        Connection.stream!(conn, stmt, values: [id], page_size: 2)
        |> Enum.take(5)
        |> Enum.map(& &1["n"])
      end)

      assert values == Enum.into(1..5, [])
      assert count2 >= 3

      assert count1 > count2
    end

  end

  test "lwt", %{conn: conn} do
    id = uuid()

    {:ok, %{kind: :rows, rows: [row]}} = Connection.execute(conn,
      "insert into test.collections (id, m, s, l, t) values (:id, :m, :s, :l, :t) if not exists",
      values: %{
        id: id,
        m: %{1 => "one"},
        s: MapSet.new([1, 2, 3, 3]),
        l: ["one", "one", "two"],
        t: {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
      }
    )

    assert row["[applied]"] == true
    assert row["id"] == nil
    assert row["m"] == nil
    assert row["s"] == nil
    assert row["l"] == nil
    assert row["t"] == nil

    {:ok, %{kind: :rows, rows: [row]}} = Connection.execute(conn,
      "insert into test.collections (id, m, s, l, t) values (:id, :m, :s, :l, :t) if not exists",
      values: %{
        id: id,
        m: %{1 => "one"},
        s: MapSet.new([1, 2, 3, 3]),
        l: ["one", "one", "two"],
        t: {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
      }
    )

    assert row["[applied]"] == false
    assert row["id"] == id
    assert row["m"] == %{ 1 => "one" }
    assert row["s"] == MapSet.new([1, 2, 3])
    assert row["l"] == ["one", "one", "two"]
    assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
  end

  test "bad type", %{conn: conn} do
    {:error, error} = Connection.execute(conn,
      "insert into test.collections (id, m) values (?, ?)",
      values: [
        {:int, 1},
        {{:map, :short, :text}, %{1 => "one"}}
      ]
    )
    assert error.message == "unrecognized CQL type 'short'"
    assert error.type == :client_library
    assert error.code == -1
  end

  @tag :focus
  test "large data", %{conn: conn, xand: xand} do
    id = uuid()
    b = :rand.bytes(10 * 1024 * 1025)

    {time, _} = :timer.tc fn ->
      Connection.execute!(conn, "insert into test.large (id, b) values (?, ?)",
        values: [id, {:blob, b}]
      )
    end

    IO.puts "insert #{round(time/1000)}ms"

    t1 = Task.async(fn ->
      {time, _} = :timer.tc fn ->
        Connection.execute!(conn, "select b from test.large where id = ?", values: [id])
      end
      time
    end)

    t2 = Task.async(fn ->
      {time, _} = :timer.tc fn ->
        Connection.execute!(conn, "select b from test.large where id = ?", values: [id])
      end
      time
    end)

    t1 = Task.await(t1)
    t2 = Task.await(t2)

    IO.puts "select1 #{round(t1/1000)}ms"
    IO.puts "select2 #{round(t2/1000)}ms"

    t1 = Task.async(fn ->
      {time, _} = :timer.tc fn ->
        Xandra.execute!(xand, "select b from test.large where id = ?", [{"uuid", id}])
      end
      time
    end)

    t2 = Task.async(fn ->
      {time, _} = :timer.tc fn ->
        Xandra.execute!(xand, "select b from test.large where id = ?", [{"uuid", id}])
      end
      time
    end)

    t1 = Task.await(t1)
    t2 = Task.await(t2)

    IO.puts "select1 #{round(t1/1000)}ms"
    IO.puts "select2 #{round(t2/1000)}ms"
  end

  defp count_events(msg, f) do
    id = uuid()
    pid = self()

    :telemetry.attach(id, msg, fn _, _, _, _ -> send(pid, msg) end, nil)

    value = try do
      f.()
    after
      :telemetry.detach(id)
    end

    count = Stream.repeatedly(fn ->
      receive do
        ^msg -> 1
      after
        0 -> 0
      end
    end)
    |> Enum.reduce_while(0, fn i, acc ->
      if i == 0 do
        {:halt, acc}
      else
        {:cont, acc+1}
      end
    end)

    {count, value}
  end

end
