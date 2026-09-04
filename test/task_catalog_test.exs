defmodule Absurd.TaskCatalogTest do
  use ExUnit.Case, async: true

  alias Absurd.Error
  alias Absurd.TaskCatalog

  defmodule InheritedQueueTask do
    use Absurd.Task, name: "inherited"

    @impl Absurd.Task
    def run(params, _context), do: {:ok, params}
  end

  defmodule ExplicitQueueTask do
    use Absurd.Task, name: "explicit", queue: "jobs"

    @impl Absurd.Task
    def run(params, _context), do: {:ok, params}
  end

  defmodule DuplicateTask do
    use Absurd.Task, name: "inherited", queue: "jobs"

    @impl Absurd.Task
    def run(params, _context), do: {:ok, params}
  end

  defmodule Catalog do
    @behaviour TaskCatalog

    @impl TaskCatalog
    def tasks, do: [InheritedQueueTask, ExplicitQueueTask]
  end

  test "builds an immutable lookup from a list or catalog module" do
    for source <- [[InheritedQueueTask, ExplicitQueueTask], Catalog] do
      assert {:ok, catalog} = TaskCatalog.new(source, "jobs")
      assert catalog.queue == "jobs"
      assert TaskCatalog.size(catalog) == 2
      assert TaskCatalog.fetch(catalog, "inherited") == InheritedQueueTask
      assert TaskCatalog.fetch(catalog, "explicit") == ExplicitQueueTask
      assert TaskCatalog.fetch(catalog, "unknown") == nil
    end
  end

  test "rejects duplicate durable names" do
    assert {:error,
            %Error{
              kind: :configuration,
              operation: :build_task_catalog,
              metadata: %{
                task_name: "inherited",
                modules: [InheritedQueueTask, DuplicateTask]
              }
            }} = TaskCatalog.new([InheritedQueueTask, DuplicateTask], "jobs")
  end

  test "rejects an explicit task queue that differs from the pool" do
    assert {:error,
            %Error{
              kind: :configuration,
              metadata: %{task_queue: "jobs", pool_queue: "other"}
            }} = TaskCatalog.new([ExplicitQueueTask], "other")
  end

  test "rejects entries and catalog modules that do not implement the contracts" do
    assert {:error, %Error{kind: :configuration}} = TaskCatalog.new([String], "jobs")
    assert {:error, %Error{kind: :configuration}} = TaskCatalog.new([:not_a_module], "jobs")
    assert {:error, %Error{kind: :configuration}} = TaskCatalog.new(__MODULE__, "jobs")
  end
end
