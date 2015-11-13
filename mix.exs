defmodule Pooler.Mixfile do
  use Mix.Project

  @version File.read!("VERSION") |> String.strip

  def project do
    [app: :pooler,
     version: @version,
     description: "An OTP Process Pool Application",
     package: package]
  end

  defp package do
    [files: ~w(src rebar.config rebar.config.script Makefile concrete.mk README.org LICENSE VERSION),
     maintainers: ["Seth Falcon"],
     licenses: ["Apache 2.0"],
     links: %{"GitHub" => "https://github.com/seth/pooler"}]
  end
end
