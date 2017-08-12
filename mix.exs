defmodule Pooler.Mixfile do
  use Mix.Project

  @version File.read!("VERSION") |> String.trim

  def project do
    [app: :pooler,
     version: @version,
     description: "An OTP Process Pool Application",
     package: package()]
  end

  defp package do
    [files: ~w(bench doc src test cover.spec config/demo.config LICENSE Makefile NEWS.org config/pooler-example.config README.org rebar.config VERSION),
     maintainers: ["Seth Falcon"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/seth/pooler"}]
  end
end
