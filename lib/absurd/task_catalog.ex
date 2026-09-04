defmodule Absurd.TaskCatalog do
  @moduledoc """
  Validated task lookup for one worker-pool queue.

  A catalog is built from an explicit list of `Absurd.Task` modules or from a
  module implementing this module's `c:tasks/0` callback. Construction loads
  task metadata, requires a `c:Absurd.Task.run/2` implementation, rejects queue
  mismatches, and rejects duplicate durable names before a worker can claim
  work.

  Catalogs are immutable and local to their worker pool. There is no global
  module scan or runtime atom creation.

  ## Examples

      iex> {:ok, catalog} = Absurd.TaskCatalog.new([], "default")
      iex> {catalog.queue, Absurd.TaskCatalog.size(catalog)}
      {"default", 0}

  An application catalog keeps a larger worker configuration focused:

      defmodule MyApp.AbsurdTasks do
        @behaviour Absurd.TaskCatalog

        @impl Absurd.TaskCatalog
        def tasks, do: [MyApp.SendEmail, MyApp.GenerateInvoice]
      end

  """

  alias Absurd.Error
  alias Absurd.Name

  @enforce_keys [:queue, :tasks]
  defstruct @enforce_keys

  @typedoc "An immutable durable-task lookup for one queue."
  @type t :: %__MODULE__{queue: String.t(), tasks: %{optional(String.t()) => module()}}

  @doc "Returns the task modules supplied by an application catalog module."
  @callback tasks() :: [module()]

  @doc """
  Builds and validates a catalog for `queue`.

  `source` may be a list of task modules or a module implementing `c:tasks/0`.
  Task metadata with no queue inherits `queue`; explicit metadata must match it.
  """
  @spec new([module()] | module(), String.t()) :: {:ok, t()} | {:error, Error.t()}
  def new(source, queue) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, modules} <- source_modules(source),
         {:ok, tasks} <- build_tasks(modules, queue) do
      {:ok, %__MODULE__{queue: queue, tasks: tasks}}
    end
  end

  @doc "Returns the module registered for a durable task name, or `nil`."
  @spec fetch(t(), String.t()) :: module() | nil
  def fetch(%__MODULE__{tasks: tasks}, task_name) when is_binary(task_name) do
    Map.get(tasks, task_name)
  end

  @doc "Returns the number of registered task names."
  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{tasks: tasks}), do: map_size(tasks)

  defp source_modules(modules) when is_list(modules), do: {:ok, modules}

  defp source_modules(module) when is_atom(module) do
    # Execute an application catalog once during pool startup. Turning failures
    # into configuration errors keeps bad catalogs from surfacing after claims.
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :tasks, 0),
         modules when is_list(modules) <- module.tasks() do
      {:ok, modules}
    else
      _other -> configuration_error("task catalog module must return a list from tasks/0", module)
    end
  rescue
    exception -> configuration_error("task catalog callback failed", module, exception)
  end

  defp source_modules(source) do
    configuration_error("tasks must be a list or task catalog module", source)
  end

  defp build_tasks(modules, queue) do
    # Build the complete immutable dispatch map before exposing it to runners.
    # reduce_while stops at the first invalid entry without returning a partial
    # catalog that could defer otherwise-known tasks.
    Enum.reduce_while(modules, {:ok, %{}}, fn module, {:ok, tasks} ->
      case validate_task_module(module, queue) do
        {:ok, task_name} -> put_task(tasks, task_name, module)
        {:error, %Error{}} = error -> {:halt, error}
      end
    end)
  end

  defp validate_task_module(module, queue) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__absurd_task__, 0),
         true <- function_exported?(module, :run, 2),
         metadata when is_map(metadata) <- module.__absurd_task__(),
         {:ok, task_name} <- Name.validate_durable(metadata[:name], :task),
         :ok <- validate_task_queue(metadata[:queue], queue, module) do
      {:ok, task_name}
    else
      {:error, %Error{}} = error -> error
      _other -> configuration_error("module must implement Absurd.Task", module)
    end
  rescue
    exception -> configuration_error("task metadata callback failed", module, exception)
  end

  defp validate_task_module(module, _queue) do
    configuration_error("task entries must be modules", module)
  end

  defp validate_task_queue(nil, _queue, _module), do: :ok
  defp validate_task_queue(queue, queue, _module), do: :ok

  defp validate_task_queue(task_queue, pool_queue, module) do
    configuration_error(
      "task queue does not match worker-pool queue",
      module,
      nil,
      %{task_queue: task_queue, pool_queue: pool_queue}
    )
  end

  defp put_task(tasks, task_name, module) do
    # Durable names, rather than Elixir modules, are the wire protocol. Duplicates
    # would make dispatch depend on list order, so reject the catalog as ambiguous.
    case tasks do
      %{^task_name => existing} ->
        error =
          configuration_error(
            "duplicate durable task name",
            module,
            nil,
            %{task_name: task_name, modules: [existing, module]}
          )

        {:halt, error}

      _other ->
        {:cont, {:ok, Map.put(tasks, task_name, module)}}
    end
  end

  defp configuration_error(message, source, cause \\ nil, metadata \\ %{}) do
    {:error,
     Error.new(:configuration, message,
       operation: :build_task_catalog,
       cause: cause,
       metadata: Map.put(metadata, :source, source)
     )}
  end
end
