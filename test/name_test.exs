defmodule Absurd.NameTest do
  use ExUnit.Case, async: true

  test "matches the permissive upstream queue-name contract" do
    assert {:ok, "   "} = Absurd.Name.validate_queue("   ")
    assert {:ok, "Queue Name-1"} = Absurd.Name.validate_queue("Queue Name-1")
    assert {:error, %Absurd.Error{kind: :validation}} = Absurd.Name.validate_queue("")
  end

  test "measures queue limits in UTF-8 bytes" do
    assert {:ok, _name} = Absurd.Name.validate_queue(String.duplicate("é", 28) <> "a")

    assert {:error, %Absurd.Error{metadata: %{max_bytes: 57}}} =
             Absurd.Name.validate_queue(String.duplicate("é", 29))
  end

  test "rejects whitespace-only durable names" do
    assert {:error, %Absurd.Error{metadata: %{field: :task}}} =
             Absurd.Name.validate_durable("  ", :task)
  end

  test "rejects invalid UTF-8 before PostgreSQL sees it" do
    assert {:error, %Absurd.Error{message: "queue must be valid UTF-8"}} =
             Absurd.Name.validate_queue(<<255>>)
  end
end
