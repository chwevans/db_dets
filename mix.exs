defmodule DbDets.Mixfile do
  use Mix.Project

  def project do
    [
      app: Db.Dets,
      version: "0.0.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      consolidate_protocols: Mix.env == :prod,
      test_coverage: [tool: ExCoveralls],
      deps: deps
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      mod: {Db.Dets.App, []},
      applications: [:logger],
    ]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:excoveralls, "~> 0.5", only: :test},
      {Db, git: "git://github.com/chwevans/db.git"},
    ]
  end
end
