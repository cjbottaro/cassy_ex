defmodule Cassandra.FrameTest do
  use ExUnit.Case
  alias Cassandra.Frame
  doctest Frame

  describe "to_iodata" do

    test "invalid types" do
      {:error, msg} = %Frame.Query{
        query: "insert into foo (id) values (?)",
        values: [{:short, 1}]
      }
      |> Frame.to_iodata()

      assert msg == "unrecognized CQL type 'short'"
    end

  end
end
