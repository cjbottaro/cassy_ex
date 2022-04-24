defmodule Cassandra do
  @moduledoc false

  defmacro __using__(_opts \\ []) do
    quote do
      alias Cassandra.{
        Client,
        Connection,
        Error,
        Frame,
        Result
      }

      import Cassandra.Utils
      require Logger
    end
  end

end
