defmodule Absurd.SQLTest do
  use ExUnit.Case, async: true

  test "pins the supported upstream schema" do
    assert Absurd.SQL.supported_schema_version() == "0.5.0"
  end

  test "validates queue creation before querying" do
    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.SQL.create_queue(self(), "default", storage_mode: :unknown)
  end

  test "validates spawn options before querying" do
    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.SQL.spawn_task(self(), "default", "task", nil,
               retry_strategy: [kind: :exponential, factor: -1]
             )
  end

  test "validates claim bounds before querying" do
    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.SQL.claim_tasks(self(), "default", "worker", batch_size: 0)
  end

  test "requires an explicit cleanup TTL" do
    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.SQL.cleanup_tasks(self(), "default", [])
  end

  test "validates absolute schedule timestamps" do
    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.SQL.schedule_run(self(), "default", <<1>>, ~N[2026-01-01 00:00:00])
  end
end
