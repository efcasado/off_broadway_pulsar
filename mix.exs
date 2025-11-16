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
      {:pulsar, git: "https://github.com/efcasado/pulsar-elixir.git", ref: "3b4f162a0c8e69506e33cc81c1bf9eb87e1f8643"},
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false}
    ]
  end
end
