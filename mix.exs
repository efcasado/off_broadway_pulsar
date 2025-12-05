defmodule OffBroadwayPulsar.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_pulsar,
      version: "1.0.3",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      consolidate_protocols: Mix.env() != :test,
      dialyzer: [
        plt_add_apps: [:pulsar]
      ],
      description: description(),
      package: package(),
      source_url: "https://github.com/efcasado/off_broadway_pulsar",
      docs: [
        main: "OffBroadway.Pulsar.Producer",
        extras: ["README.md"]
      ]
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
      {:pulsar, "~> 2.2.6", hex: :pulsar_elixir},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.39.1", only: :dev, runtime: false},
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "A Broadway producer for Apache Pulsar."
  end

  defp package do
    [
      name: "off_broadway_pulsar",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      licenses: ~w(MIT),
      links: %{
        "GitHub" => "https://github.com/efcasado/off_broadway_pulsar",
        "Changelog" => "https://github.com/efcasado/off_broadway_pulsar/blob/main/CHANGELOG.md"
      }
    ]
  end
end
