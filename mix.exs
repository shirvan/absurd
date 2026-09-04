defmodule Absurd.MixProject do
  use Mix.Project

  @spec project() :: keyword()
  def project do
    [
      app: :absurd,
      version: "0.2.0",
      description: "Unofficial Elixir SDK and OTP workers for Absurd",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      package: package(),
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      deps: deps(),
      docs: docs(),
      aliases: aliases()
    ]
  end

  @spec application() :: keyword()
  def application do
    [
      extra_applications: [:logger],
      mod: {Absurd.Application, []}
    ]
  end

  @spec cli() :: keyword()
  def cli do
    [preferred_envs: [check: :test]]
  end

  defp deps do
    [
      {:postgrex, "~> 0.22.4"},
      {:jason, "~> 1.4.5"},
      {:telemetry, "~> 1.4.2"},
      {:credo, "~> 1.7.19", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/shirvan/absurd",
      source_ref: "0.2.0",
      extras: [
        "README.md",
        "ARCHITECTURE.md",
        "CONTRIBUTING.md",
        "CHANGELOG.md",
        "LICENSE"
      ]
    ]
  end

  defp package do
    [
      files:
        ~w(lib .formatter.exs mix.exs README.md ARCHITECTURE.md CONTRIBUTING.md CHANGELOG.md LICENSE),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/shirvan/absurd",
        "Absurd" => "https://github.com/earendil-works/absurd"
      }
    ]
  end

  defp aliases do
    [
      check: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --strict",
        "test",
        "docs --warnings-as-errors"
      ]
    ]
  end
end
