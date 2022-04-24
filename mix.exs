defmodule CassandraEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :cassandra_ex,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Cassandra.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:connection, "~> 1.0"},
      {:decimal, "~> 1.0 or ~> 2.0"},
      {:telemetry, "~> 1.0 or ~> 0.4"},
      {:ex_doc, "~> 0.28", only: :dev},
      {:httpoison, "~> 1.8", only: :test},
      {:jason, "~> 1.0", only: :test},
    ]
  end
end
