defmodule AttentionServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :attention_server,
      description: "Harklight Server",
      version: "0.1.21",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: [attention_server: [include_erts: true, include_executables_for: [:unix]]]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto, :public_key, :ssl],
      mod: {AttentionServer.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:exqlite, "0.39.0"},
      {:mint, "~> 1.11.0"},
      {:plug, "~> 1.20.0"},
      {:bandit, "~> 1.13.0"}
    ]
  end
end
