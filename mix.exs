defmodule Pgrx.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: releases()
    ]
  end

  defp releases do
    [
      pgrx: [
        applications: [
          wire: :permanent,
          engine: :permanent,
          router: :permanent,
          runtime_tools: :none
        ],
        strip_beams: true,
        rel_templates_path: "rel"
      ]
    ]
  end

  defp deps do
    []
  end
end
