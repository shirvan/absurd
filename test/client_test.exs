defmodule Absurd.ClientTest do
  use ExUnit.Case, async: true

  test "uses the official client defaults" do
    assert {:ok, client} = Absurd.Client.new(db: self())
    assert client.queue == "default"
    assert client.default_max_attempts == 5
    assert client.query_options == []
  end

  test "rejects missing and unknown configuration" do
    assert {:error, %Absurd.Error{kind: :configuration, metadata: %{field: :db}}} =
             Absurd.Client.new([])

    assert {:error, %Absurd.Error{kind: :configuration, metadata: %{options: [:typo]}}} =
             Absurd.Client.new(db: self(), typo: true)
  end

  test "requires an explicit queue for raw task names before querying" do
    client = Absurd.Client.new!(db: self())

    assert {:error, %Absurd.Error{kind: :configuration}} =
             Absurd.Client.spawn(client, "unregistered", %{})
  end

  test "rejects invalid JSON before querying" do
    client = Absurd.Client.new!(db: self())

    assert {:error, %Absurd.Error{kind: :validation}} =
             Absurd.Client.spawn(client, "unregistered", %{atom_key: true}, queue: "default")
  end

  test "rejects a hook module that cannot be loaded" do
    assert {:error, %Absurd.Error{kind: :configuration}} =
             Absurd.Client.new(db: self(), hooks: MyApp.MissingAbsurdHooks)
  end

  test "a spawn-result queue cannot be silently overridden" do
    client = Absurd.Client.new!(db: self())

    spawned = %Absurd.SpawnResult{
      queue: "one",
      task_id: <<1>>,
      run_id: <<2>>,
      attempt: 1,
      created: true
    }

    assert {:error, %Absurd.Error{kind: :configuration}} =
             Absurd.Client.fetch_task_result(client, spawned, queue: "two")
  end
end
