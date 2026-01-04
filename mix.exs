defmodule MOQX.MixProject do
  use Mix.Project

  def project do
    [
      app: :moqx,
      description: description(),
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      source_url: "https://github.com/kim_company/moqx",
      homepage_url: "https://github.com/kim_company/moqx",
      docs: docs(),
      package: package(),
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.30", only: :dev, runtime: false}
    ]
  end

  defp description do
    "MOQT (Media over QUIC Transport) protocol primitives for Elixir."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      maintainers: ["KIM Keep In Mind GmbH"],
      links: %{"GitHub" => "https://github.com/kim_company/moqx"},
      files: ["lib", "mix.exs", "README.md", "LICENSE"]
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"]
    ]
  end
end
