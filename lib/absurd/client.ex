defmodule Absurd.Client do
  @moduledoc """
  Process-free configuration for calls to `Absurd.SQL`.

  A client identifies a caller-owned Postgrex queryable and carries safe
  defaults. It does not own a connection pool, install database objects, or
  start workers.

  Registered `Absurd.Task` modules provide their stable task name and queue.
  Explicit string names support tasks implemented by another Absurd SDK when
  the queue is supplied. Spawn results can then be passed directly to fetch,
  await, retry, and cancellation operations.

  With a database containing the supported schema, a typical flow is:

      client = Absurd.Client.new!(db: MyApp.AbsurdDB, queue: "email")
      :ok = Absurd.Client.create_queue(client)

      {:ok, spawned} =
        Absurd.Client.spawn(
          client,
          MyApp.SendEmail,
          %{"user_id" => "usr_123"},
          idempotency_key: "email:usr_123"
        )

      {:ok, terminal} =
        Absurd.Client.await_task_result(client, spawned, timeout: 30_000)

  ## Examples

      iex> {:ok, client} = Absurd.Client.new(db: self())
      iex> {client.queue, client.default_max_attempts}
      {"default", 5}

      iex> match?({:error, %Absurd.Error{kind: :validation}}, Absurd.Client.new(db: self(), queue: ""))
      true

  """

  alias Absurd.Error
  alias Absurd.Name
  alias Absurd.QueuePolicy
  alias Absurd.SpawnResult
  alias Absurd.SQL
  alias Absurd.TaskResult

  @default_queue "default"
  @default_max_attempts 5
  @option_defaults [
    db: nil,
    queue: @default_queue,
    default_max_attempts: @default_max_attempts,
    hooks: nil,
    query_options: []
  ]
  @spawn_option_defaults [
    max_attempts: nil,
    retry_strategy: nil,
    headers: nil,
    queue: nil,
    cancellation: nil,
    idempotency_key: nil
  ]
  @result_option_defaults [queue: nil]
  @await_option_defaults [queue: nil, timeout: :infinity]
  @retry_option_defaults [queue: nil, max_attempts: nil, spawn_new: false]
  @queue_create_defaults [
    storage_mode: :unpartitioned,
    partition_lookahead: nil,
    partition_lookback: nil,
    cleanup_ttl: nil,
    cleanup_limit: nil,
    detach_mode: nil,
    detach_min_age: nil
  ]

  @enforce_keys [:db, :queue, :default_max_attempts]
  defstruct [:db, :queue, :default_max_attempts, :hooks, query_options: []]

  @typedoc "A Postgrex pool, connection, registered name, or checked-out connection."
  @type queryable :: GenServer.server()

  @typedoc "An immutable Absurd client value."
  @type t :: %__MODULE__{
          db: queryable(),
          queue: String.t(),
          default_max_attempts: pos_integer(),
          hooks: module() | nil,
          query_options: keyword()
        }

  @typedoc "A task module, stable task name, spawn result, or task identifier."
  @type task_reference :: module() | String.t() | SpawnResult.t() | binary()

  @doc "Returns the default queue used by new clients."
  @spec default_queue() :: String.t()
  def default_queue, do: @default_queue

  @doc "Returns the default maximum number of task attempts."
  @spec default_max_attempts() :: pos_integer()
  def default_max_attempts, do: @default_max_attempts

  @doc """
  Builds an immutable client around a caller-owned Postgrex queryable.

  ## Options

    * `:db` - required Postgrex queryable;
    * `:queue` - default queue, initially `"default"`;
    * `:default_max_attempts` - positive integer, initially `5`;
    * `:hooks` - optional module implementing `Absurd.Hooks`;
    * `:query_options` - options passed to Postgrex queries.

  Unknown options are rejected.
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(options) when is_list(options) do
    with {:ok, validated} <- validate_options(options),
         {:ok, queue} <- Name.validate_queue(validated[:queue]),
         :ok <- validate_db(validated[:db]),
         :ok <- validate_max_attempts(validated[:default_max_attempts]),
         :ok <- validate_hooks(validated[:hooks]),
         :ok <- validate_query_options(validated[:query_options]) do
      {:ok,
       %__MODULE__{
         db: validated[:db],
         queue: queue,
         default_max_attempts: validated[:default_max_attempts],
         hooks: validated[:hooks],
         query_options: validated[:query_options]
       }}
    end
  end

  def new(_options), do: {:error, configuration_error("client options must be a keyword list")}

  @doc """
  Builds a client and raises `ArgumentError` when its static configuration is
  invalid.
  """
  @spec new!(keyword()) :: t()
  def new!(options) do
    case new(options) do
      {:ok, client} -> client
      {:error, error} -> raise ArgumentError, "invalid Absurd client: #{error.message}"
    end
  end

  @doc """
  Spawns a registered task module or explicit stable task name.

  A raw task name requires `:queue`. Registered modules use their task metadata;
  a queue override must equal the registered queue. Per-spawn defaults take
  precedence over task metadata and then the client maximum-attempt default.
  """
  @spec spawn(t(), module() | String.t(), Absurd.JSON.value(), keyword()) ::
          {:ok, SpawnResult.t()} | {:error, Error.t()}
  def spawn(%__MODULE__{} = client, task, params, options \\ []) do
    with {:ok, task_name} <- task_name(task),
         {:ok, options} <- validate_spawn_options(options),
         {:ok, options} <- run_before_spawn(client.hooks, task_name, params, options),
         {:ok, options} <- validate_spawn_options(options),
         {:ok, queue, options} <- resolve_spawn(client, task, options) do
      SQL.spawn_task(
        client.db,
        queue,
        task_name,
        params,
        Keyword.put(options, :query_options, client.query_options)
      )
    end
  end

  @doc """
  Fetches the current result snapshot for a task.

  Passing an `Absurd.SpawnResult` uses its queue. Passing a task ID uses the
  client queue unless `:queue` is supplied. An unknown task returns `{:ok, nil}`.
  """
  @spec fetch_task_result(t(), SpawnResult.t() | binary(), keyword()) ::
          {:ok, TaskResult.t() | nil} | {:error, Error.t()}
  def fetch_task_result(%__MODULE__{} = client, task, options \\ []) do
    with {:ok, options} <-
           validate_client_options(options, @result_option_defaults, :fetch_task_result),
         {:ok, queue, task_id} <- resolve_task_reference(client, task, options[:queue]) do
      SQL.get_task_result(client.db, queue, task_id, client.query_options)
    end
  end

  @doc """
  Polls until a task reaches a terminal state.

  Polling starts at 50 milliseconds and doubles to at most one second. The
  `:timeout` is an Elixir duration in milliseconds or `:infinity`; timing out
  does not cancel the durable task.
  """
  @spec await_task_result(t(), SpawnResult.t() | binary(), keyword()) ::
          {:ok, TaskResult.t()} | {:error, Error.t()}
  def await_task_result(%__MODULE__{} = client, task, options \\ []) do
    with {:ok, options} <-
           validate_client_options(options, @await_option_defaults, :await_task_result),
         :ok <- validate_timeout(options[:timeout]),
         {:ok, queue, task_id} <- resolve_task_reference(client, task, options[:queue]) do
      started_at = System.monotonic_time(:millisecond)
      await_poll(client, queue, task_id, options[:timeout], started_at, 50)
    end
  end

  @doc """
  Retries a failed task.

  `:max_attempts` overrides the retry limit. `:spawn_new` creates a new logical
  task rather than extending the failed task in place.
  """
  @spec retry_task(t(), SpawnResult.t() | binary(), keyword()) ::
          {:ok, SpawnResult.t()} | {:error, Error.t()}
  def retry_task(%__MODULE__{} = client, task, options \\ []) do
    with {:ok, options} <- validate_client_options(options, @retry_option_defaults, :retry_task),
         {:ok, queue, task_id} <- resolve_task_reference(client, task, options[:queue]) do
      retry_options =
        options
        |> Keyword.delete(:queue)
        |> Keyword.put(:query_options, client.query_options)

      SQL.retry_task(client.db, queue, task_id, retry_options)
    end
  end

  @doc "Cancels a task while leaving already-started external effects cooperative."
  @spec cancel_task(t(), SpawnResult.t() | binary(), keyword()) :: :ok | {:error, Error.t()}
  def cancel_task(%__MODULE__{} = client, task, options \\ []) do
    with {:ok, options} <-
           validate_client_options(options, @result_option_defaults, :cancel_task),
         {:ok, queue, task_id} <- resolve_task_reference(client, task, options[:queue]) do
      SQL.cancel_task(client.db, queue, task_id, client.query_options)
    end
  end

  @doc "Emits a first-write-wins event on the selected or default queue."
  @spec emit_event(t(), String.t(), Absurd.JSON.value(), keyword()) ::
          :ok | {:error, Error.t()}
  def emit_event(%__MODULE__{} = client, event_name, payload, options \\ []) do
    with {:ok, options} <- validate_client_options(options, @result_option_defaults, :emit_event),
         {:ok, queue} <- resolve_queue(client, options[:queue]) do
      SQL.emit_event(client.db, queue, event_name, payload, client.query_options)
    end
  end

  @doc """
  Creates the client's default queue.

  Queue creation accepts `:storage_mode` and the official queue-policy fields.
  """
  @spec create_queue(t()) :: :ok | {:error, Error.t()}
  def create_queue(%__MODULE__{} = client), do: create_queue(client, client.queue, [])

  @doc "Creates a named queue with default storage and maintenance policy."
  @spec create_queue(t(), String.t()) :: :ok | {:error, Error.t()}
  def create_queue(%__MODULE__{} = client, queue), do: create_queue(client, queue, [])

  @doc "Creates a named queue with storage and maintenance options."
  @spec create_queue(t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def create_queue(%__MODULE__{} = client, queue, options) do
    with {:ok, options} <-
           validate_client_options(options, @queue_create_defaults, :create_queue),
         {storage_mode, policy} <- Keyword.pop(options, :storage_mode),
         :ok <-
           SQL.create_queue(client.db, queue,
             storage_mode: storage_mode,
             query_options: client.query_options
           ) do
      maybe_set_queue_policy(client, queue, policy)
    end
  end

  @doc "Lists every Absurd queue visible to the client's database connection."
  @spec list_queues(t()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def list_queues(%__MODULE__{} = client) do
    SQL.list_queues(client.db, client.query_options)
  end

  @doc "Returns the default queue's policy, or `nil` when it does not exist."
  @spec get_queue_policy(t()) :: {:ok, QueuePolicy.t() | nil} | {:error, Error.t()}
  def get_queue_policy(%__MODULE__{} = client), do: get_queue_policy(client, client.queue)

  @doc "Returns a named queue's policy, or `nil` when it does not exist."
  @spec get_queue_policy(t(), String.t()) :: {:ok, QueuePolicy.t() | nil} | {:error, Error.t()}
  def get_queue_policy(%__MODULE__{} = client, queue) do
    SQL.get_queue_policy(client.db, queue, client.query_options)
  end

  @doc "Updates the default queue's official maintenance-policy fields."
  @spec set_queue_policy(t(), keyword()) :: :ok | {:error, Error.t()}
  def set_queue_policy(%__MODULE__{} = client, policy) do
    set_queue_policy(client, client.queue, policy)
  end

  @doc "Updates a named queue's official maintenance-policy fields."
  @spec set_queue_policy(t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def set_queue_policy(%__MODULE__{} = client, queue, policy) do
    SQL.set_queue_policy(client.db, queue, policy, client.query_options)
  end

  @doc "Drops the client's default queue if it exists."
  @spec drop_queue(t()) :: :ok | {:error, Error.t()}
  def drop_queue(%__MODULE__{} = client), do: drop_queue(client, client.queue)

  @doc "Drops a named queue if it exists."
  @spec drop_queue(t(), String.t()) :: :ok | {:error, Error.t()}
  def drop_queue(%__MODULE__{} = client, queue) do
    SQL.drop_queue(client.db, queue, client.query_options)
  end

  defp task_name(task) when is_binary(task), do: Name.validate_durable(task, :task)

  defp task_name(task) when is_atom(task) do
    with {:module, ^task} <- Code.ensure_loaded(task),
         true <- function_exported?(task, :__absurd_task__, 0) do
      Name.validate_durable(task.__absurd_task__().name, :task)
    else
      _reason -> unknown_task_module(task)
    end
  end

  defp task_name(_task) do
    {:error,
     Error.new(:unknown_task, "task must be a task module or stable name", operation: :spawn)}
  end

  defp unknown_task_module(task) do
    {:error,
     Error.new(:unknown_task, "module does not define an Absurd task",
       operation: :spawn,
       metadata: %{module: task}
     )}
  end

  defp validate_spawn_options(options) do
    validate_client_options(options, @spawn_option_defaults, :spawn)
  end

  defp resolve_spawn(client, task, options) when is_atom(task) do
    metadata = task.__absurd_task__()
    registered_queue = metadata.queue || client.queue

    with {:ok, registered_queue} <- Name.validate_queue(registered_queue),
         :ok <- validate_queue_override(options[:queue], registered_queue) do
      resolved =
        options
        |> Keyword.delete(:queue)
        |> put_default(
          :max_attempts,
          metadata.default_max_attempts || client.default_max_attempts
        )
        |> put_default(:cancellation, metadata.default_cancellation)

      {:ok, registered_queue, resolved}
    end
  end

  defp resolve_spawn(client, task, options) when is_binary(task) do
    with {:ok, queue} <- require_raw_task_queue(options[:queue]) do
      resolved =
        options
        |> Keyword.delete(:queue)
        |> put_default(:max_attempts, client.default_max_attempts)

      {:ok, queue, resolved}
    end
  end

  defp validate_queue_override(nil, _registered_queue), do: :ok
  defp validate_queue_override(queue, queue), do: :ok

  defp validate_queue_override(requested, registered) do
    {:error,
     Error.new(:configuration, "spawn queue does not match the task registration",
       operation: :spawn,
       metadata: %{requested_queue: requested, registered_queue: registered}
     )}
  end

  defp require_raw_task_queue(nil) do
    {:error,
     Error.new(:configuration, "a raw task name requires an explicit :queue", operation: :spawn)}
  end

  defp require_raw_task_queue(queue), do: Name.validate_queue(queue)

  defp resolve_task_reference(_client, %SpawnResult{} = spawned, nil) do
    {:ok, spawned.queue, spawned.task_id}
  end

  defp resolve_task_reference(_client, %SpawnResult{} = spawned, spawned_queue)
       when spawned_queue == spawned.queue do
    {:ok, spawned.queue, spawned.task_id}
  end

  defp resolve_task_reference(_client, %SpawnResult{} = spawned, _queue) do
    {:error,
     Error.new(:configuration, "queue override does not match the spawn result",
       metadata: %{queue: spawned.queue}
     )}
  end

  defp resolve_task_reference(client, task_id, queue) when is_binary(task_id) do
    with {:ok, queue} <- resolve_queue(client, queue) do
      {:ok, queue, task_id}
    end
  end

  defp resolve_task_reference(_client, _task, _queue) do
    {:error, Error.new(:validation, "task reference must be a spawn result or task ID")}
  end

  defp resolve_queue(client, nil), do: {:ok, client.queue}
  defp resolve_queue(_client, queue), do: Name.validate_queue(queue)

  defp put_default(options, key, value) do
    if is_nil(options[key]) and not is_nil(value),
      do: Keyword.put(options, key, value),
      else: options
  end

  defp run_before_spawn(nil, _task_name, _params, options), do: {:ok, options}

  defp run_before_spawn(hooks, task_name, params, options) do
    if function_exported?(hooks, :before_spawn, 3) do
      run_before_spawn_hook(hooks, task_name, params, options)
    else
      {:ok, options}
    end
  end

  defp run_before_spawn_hook(hooks, task_name, params, options) do
    case hooks.before_spawn(task_name, params, options) do
      {:ok, hooked_options} -> {:ok, hooked_options}
      {:error, reason} -> hook_error(:before_spawn, reason)
      other -> hook_error(:before_spawn, {:invalid_return, other})
    end
  rescue
    exception -> hook_error(:before_spawn, exception)
  end

  defp hook_error(hook, reason) do
    {:error,
     Error.new(:configuration, "Absurd hook failed",
       operation: hook,
       cause: reason,
       metadata: %{hook: hook}
     )}
  end

  defp await_poll(client, queue, task_id, timeout, started_at, delay) do
    case SQL.get_task_result(client.db, queue, task_id, client.query_options) do
      {:ok, %TaskResult{} = snapshot} ->
        await_snapshot(client, snapshot, queue, task_id, timeout, started_at, delay)

      {:ok, nil} ->
        {:error,
         Error.new(:unknown_task, "task result was not found",
           operation: :await_task_result,
           metadata: %{queue: queue, task_id: task_id}
         )}

      {:error, error} ->
        {:error, error}
    end
  end

  defp await_snapshot(
         _client,
         %TaskResult{} = snapshot,
         _queue,
         _task_id,
         _timeout,
         _started_at,
         _delay
       )
       when snapshot.state in [:completed, :failed, :cancelled],
       do: {:ok, snapshot}

  defp await_snapshot(client, _snapshot, queue, task_id, timeout, started_at, delay) do
    await_again(client, queue, task_id, timeout, started_at, delay)
  end

  defp await_again(client, queue, task_id, timeout, started_at, delay) do
    case remaining_timeout(timeout, started_at) do
      0 ->
        {:error,
         Error.new(:timeout, "timed out waiting for task result",
           operation: :await_task_result,
           metadata: %{queue: queue, task_id: task_id}
         )}

      remaining ->
        sleep_for = if remaining == :infinity, do: delay, else: min(delay, remaining)
        Process.sleep(sleep_for)
        await_poll(client, queue, task_id, timeout, started_at, min(delay * 2, 1_000))
    end
  end

  defp remaining_timeout(:infinity, _started_at), do: :infinity

  defp remaining_timeout(timeout, started_at) do
    elapsed = System.monotonic_time(:millisecond) - started_at
    max(timeout - elapsed, 0)
  end

  defp validate_timeout(:infinity), do: :ok
  defp validate_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: :ok

  defp validate_timeout(_timeout) do
    {:error, Error.new(:validation, "timeout must be a non-negative integer or :infinity")}
  end

  defp maybe_set_queue_policy(client, queue, policy) do
    if Enum.all?(policy, fn {_key, value} -> is_nil(value) end) do
      :ok
    else
      SQL.set_queue_policy(client.db, queue, policy, client.query_options)
    end
  end

  defp validate_client_options(options, defaults, operation) when is_list(options) do
    if Keyword.keyword?(options) do
      case Keyword.validate(options, defaults) do
        {:ok, validated} ->
          {:ok, validated}

        {:error, unknown} ->
          {:error,
           Error.new(:validation, "unknown options",
             operation: operation,
             metadata: %{options: unknown}
           )}
      end
    else
      {:error, Error.new(:validation, "options must be a keyword list", operation: operation)}
    end
  end

  defp validate_client_options(_options, _defaults, operation) do
    {:error, Error.new(:validation, "options must be a keyword list", operation: operation)}
  end

  defp validate_options(options) do
    case Keyword.validate(options, @option_defaults) do
      {:ok, validated} ->
        {:ok, validated}

      {:error, unknown} ->
        {:error,
         configuration_error("unknown client options: #{inspect(unknown)}", %{options: unknown})}
    end
  end

  defp validate_db(nil), do: {:error, configuration_error(":db is required", %{field: :db})}
  defp validate_db(_db), do: :ok

  defp validate_max_attempts(value) when is_integer(value) and value > 0, do: :ok

  defp validate_max_attempts(_value) do
    {:error,
     configuration_error(":default_max_attempts must be a positive integer", %{
       field: :default_max_attempts
     })}
  end

  defp validate_hooks(nil), do: :ok

  defp validate_hooks(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        :ok

      {:error, reason} ->
        {:error, configuration_error(":hooks module could not be loaded", %{cause: reason})}
    end
  end

  defp validate_hooks(_module) do
    {:error, configuration_error(":hooks must be a module", %{field: :hooks})}
  end

  defp validate_query_options(options) when is_list(options) do
    if Keyword.keyword?(options) do
      :ok
    else
      {:error, configuration_error(":query_options must be a keyword list")}
    end
  end

  defp validate_query_options(_options) do
    {:error, configuration_error(":query_options must be a keyword list")}
  end

  defp configuration_error(message, metadata \\ %{}) do
    Error.new(:configuration, message, operation: :new_client, metadata: metadata)
  end
end
