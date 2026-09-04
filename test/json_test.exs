defmodule Absurd.JSONTest do
  use ExUnit.Case, async: true

  test "accepts every JSON value shape" do
    assert :ok =
             Absurd.JSON.validate(%{
               "null" => nil,
               "boolean" => true,
               "integer" => 1,
               "float" => 1.5,
               "string" => "value",
               "array" => [false, %{"nested" => 2}]
             })
  end

  test "rejects structs, atom keys, tuples, and improper lists" do
    refute Absurd.JSON.valid?(%URI{})
    refute Absurd.JSON.valid?(%{atom: "key"})
    refute Absurd.JSON.valid?({:tuple})
    refute Absurd.JSON.valid?([1 | 2])
  end

  test "reports a nested invalid path" do
    assert {:error, %Absurd.Error{metadata: %{path: ["items", 0, :bad]}}} =
             Absurd.JSON.validate(%{"items" => [%{bad: true}]})
  end
end
