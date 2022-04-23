defmodule CassandraTest do
  use ExUnit.Case
  alias Cassandra.{Connection, Result}
  doctest Connection

  setup_all do

    label_filter =Jason.encode!(%{
      label: [
        "com.docker.compose.project=cassandra_ex",
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

    {:ok, _} = Connection.execute(conn, "DROP KEYSPACE IF EXISTS cassandra_test")
    {:ok, _} = Connection.execute(conn, """
      CREATE KEYSPACE IF NOT EXISTS test
      WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)
    {:ok, _} = Connection.execute(conn, """
      CREATE TABLE IF NOT EXISTS test.collections (
        id INT PRIMARY KEY,
        m MAP<int,text>,
        s set<int>,
        l list<text>,
        t tuple<int,text,uuid>
      )
    """)

    %{conn: conn}
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
      {:ok, %Result{}} = Connection.execute(conn, """
        insert into test.collections (id, m, s, l, t) values (
          1,
          { 1 : 'one' },
          {1, 2, 3, 3},
          ['one', 'one', 'two'],
          (1, 'one', ab9dcec9-2877-46d9-9e63-be00a94ac900)
        )
      """)

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = 1"
      )

      assert row["id"] == 1
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

    test "positional", %{conn: conn} do
      {:ok, %Result{}} = Connection.execute(conn,
        "insert into test.collections (id, m, s, l, t) values (?, ?, ?, ?, ?)",
        values: [
          1,
          %{1 => "one"},
          MapSet.new([1, 2, 3, 3]),
          ["one", "one", "two"],
          {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
        ]
      )

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = 1"
      )

      assert row["id"] == 1
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

    test "named", %{conn: conn} do
      {:ok, %Result{}} = Connection.execute(conn,
        "insert into test.collections (id, m, s, l, t) values (:id, :m, :s, :l, :t)",
        values: %{
          id: 1,
          m: %{1 => "one"},
          s: MapSet.new([1, 2, 3, 3]),
          l: ["one", "one", "two"],
          t: {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
        }
      )

      {:ok, %Result{rows: [row]}} = Connection.execute(conn,
        "select * from test.collections where id = 1"
      )

      assert row["id"] == 1
      assert row["m"] == %{ 1 => "one" }
      assert row["s"] == MapSet.new([1, 2, 3])
      assert row["l"] == ["one", "one", "two"]
      assert row["t"] == {1, "one", "ab9dcec9-2877-46d9-9e63-be00a94ac900"}
    end

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

end
