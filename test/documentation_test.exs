defmodule Absurd.DocumentationTest do
  use ExUnit.Case, async: true

  test "every library module and public API has documentation" do
    modules =
      :absurd
      |> Application.spec(:modules)
      |> Enum.filter(&String.starts_with?(Atom.to_string(&1), "Elixir.Absurd"))

    assert modules != []

    Enum.each(modules, &assert_documented/1)
  end

  defp assert_documented(module) do
    assert {:docs_v1, _, :elixir, _, module_doc, _, entries} = Code.fetch_docs(module)
    assert documented?(module_doc), "missing module documentation for #{inspect(module)}"

    Enum.each(entries, fn {{kind, name, arity}, _, _, docs, metadata} ->
      if public_entry?(kind, metadata) do
        assert documented?(docs),
               "missing documentation for #{inspect(module)}.#{name}/#{arity}"
      end
    end)
  end

  defp documented?({_line, documentation}) when is_map(documentation),
    do: map_size(documentation) > 0

  defp documented?(documentation) when is_map(documentation), do: map_size(documentation) > 0
  defp documented?(:hidden), do: true
  defp documented?(_documentation), do: false

  defp public_entry?(kind, metadata) do
    kind in [:function, :macro, :callback, :macrocallback, :type] and
      not Map.has_key?(metadata, :implements)
  end
end
