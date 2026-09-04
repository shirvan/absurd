defmodule Absurd.WorkerPoolTest do
  use ExUnit.Case, async: true

  defmodule Pool do
  end

  test "provides an ordinary OTP child specification keyed by pool name" do
    assert %{
             id: {Absurd.WorkerPool, Pool},
             start: {Absurd.WorkerPool, :start_link, [options]},
             type: :supervisor
           } =
             Absurd.WorkerPool.child_spec(
               name: Pool,
               db: MyApp.DB,
               queue: "default",
               tasks: []
             )

    assert options[:name] == Pool
  end

  test "rejects unknown options before startup" do
    assert {:error,
            %Absurd.Error{
              kind: :configuration,
              operation: :start_worker_pool,
              metadata: %{options: [:extra]}
            }} =
             Absurd.WorkerPool.start_link(
               name: Pool,
               db: MyApp.DB,
               queue: "default",
               tasks: [],
               extra: true
             )
  end

  test "rejects invalid concurrency before startup" do
    assert {:error,
            %Absurd.Error{
              kind: :configuration,
              metadata: %{field: :concurrency, value: 0}
            }} =
             Absurd.WorkerPool.start_link(
               name: Pool,
               db: MyApp.DB,
               queue: "default",
               tasks: [],
               concurrency: 0
             )
  end

  test "rejects a hook module that cannot be loaded before schema access" do
    assert {:error, %Absurd.Error{kind: :configuration}} =
             Absurd.WorkerPool.start_link(
               name: Pool,
               db: self(),
               queue: "default",
               tasks: [],
               hooks: MyApp.MissingAbsurdHooks
             )
  end
end
