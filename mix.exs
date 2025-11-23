defmodule OffBroadwayPulsar.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_pulsar,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      included_applications: [:pulsar]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.2"},
      {:pulsar, git: "https://github.com/efcasado/pulsar-elixir.git", ref: "5677dd08ec8f1e7da0f81cb4bd3daa76949d4703"},
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
