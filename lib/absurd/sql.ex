defmodule Absurd.SQL do
  @moduledoc """
  Low-level, process-free wrapper around the upstream Absurd PostgreSQL
  functions.

  Every operation accepts an explicit Postgrex pool, registered name, or
  checked-out connection as its first argument. Passing a transaction
  connection therefore keeps Absurd operations in the caller's transaction;
  this module never checks out a second connection itself.

  SQL statements are fixed and all application values use Postgrex parameters.
  The module validates protocol values before issuing a query and maps database
  failures to `Absurd.Error`.

  SDK `0.2.x` targets exactly schema `0.5.0`:

      iex> Absurd.SQL.supported_schema_version()
      "0.5.0"

  Passing a checked-out connection keeps application data and task creation in
  one transaction:

      Postgrex.transaction(MyApp.AbsurdDB, fn connection ->
        Postgrex.query!(connection, "INSERT INTO orders (id) VALUES ($1)", [order_id])

        case Absurd.SQL.spawn_task(
               connection,
               "workflows",
               "fulfil-order",
               %{"order_id" => order_id},
               idempotency_key: "fulfil:\#{order_id}"
             ) do
          {:ok, spawned} -> spawned
          {:error, error} -> Postgrex.rollback(connection, error)
        end
      end)
  """

  alias Absurd.Checkpoint
  alias Absurd.ClaimedTask
  alias Absurd.CleanupResult
  alias Absurd.Error
  alias Absurd.EventWait
  alias Absurd.JSON
  alias Absurd.Name
  alias Absurd.QueuePolicy
  alias Absurd.SpawnResult
  alias Absurd.TaskResult
  alias Absurd.Telemetry

  @supported_schema_version "0.5.0"
  @spawn_option_defaults [
    max_attempts: nil,
    retry_strategy: nil,
    headers: nil,
    cancellation: nil,
    idempotency_key: nil,
    query_options: []
  ]
  @retry_option_defaults [max_attempts: nil, spawn_new: false, query_options: []]
  @queue_create_defaults [storage_mode: :unpartitioned, query_options: []]
  @queue_policy_defaults [
    partition_lookahead: nil,
    partition_lookback: nil,
    cleanup_ttl: nil,
    cleanup_limit: nil,
    detach_mode: nil,
    detach_min_age: nil
  ]
  @claim_option_defaults [claim_timeout: 120_000, batch_size: 1, query_options: []]
  @failure_option_defaults [retry_at: nil, query_options: []]
  @checkpoint_option_defaults [extend_claim_by: nil, query_options: []]
  @checkpoint_read_defaults [include_pending: false, query_options: []]
  @event_wait_defaults [timeout: :infinity, query_options: []]
  @cleanup_defaults [ttl: nil, limit: 1_000, query_options: []]
  # If the connection disappears after one of these operations was sent, the
  # client cannot know whether PostgreSQL committed it. That uncertainty is part
  # of the result contract and must not be collapsed into an ordinary DB error.
  @mutating_operations [
    :create_queue,
    :set_queue_policy,
    :drop_queue,
    :spawn_task,
    :retry_task,
    :cancel_task,
    :emit_event,
    :claim_tasks,
    :complete_run,
    :schedule_run,
    :schedule_run_after,
    :fail_run,
    :set_task_checkpoint_state,
    :extend_claim,
    :await_event,
    :cleanup_all_queues,
    :cleanup_tasks,
    :cleanup_events
  ]

  @typedoc "Options forwarded to `Postgrex.query/4`."
  @type query_options :: keyword()

  @typedoc "A retry strategy persisted in spawn options."
  @type retry_strategy ::
          [
            kind: :none | :fixed | :exponential,
            base_seconds: number(),
            factor: number(),
            max_seconds: number()
          ]

  @typedoc "A cancellation policy whose durations are expressed in seconds."
  @type cancellation :: [max_duration: non_neg_integer(), max_delay: non_neg_integer()]

  @typedoc "Options accepted by `spawn_task/5`."
  @type spawn_option ::
          {:max_attempts, pos_integer()}
          | {:retry_strategy, retry_strategy()}
          | {:headers, %{optional(String.t()) => JSON.value()}}
          | {:cancellation, cancellation()}
          | {:idempotency_key, String.t()}
          | {:query_options, query_options()}

  @doc "Returns the upstream Absurd schema version supported by this release."
  @spec supported_schema_version() :: String.t()
  def supported_schema_version, do: @supported_schema_version

  @doc "Returns the schema version reported by `absurd.get_schema_version/0`."
  @spec schema_version(Absurd.Client.queryable(), query_options()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def schema_version(db, query_options \\ []) do
    with {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <-
           query(db, "SELECT absurd.get_schema_version()", [], query_options, :schema_version) do
      case result.rows do
        [[version]] when is_binary(version) -> {:ok, version}
        rows -> unexpected_rows(:schema_version, rows)
      end
    end
  end

  @doc """
  Verifies that the database reports the supported Absurd schema version.

  A mismatch returns an `Absurd.Error` with kind `:schema_incompatible`.
  """
  @spec verify_schema_version(Absurd.Client.queryable(), query_options()) ::
          :ok | {:error, Error.t()}
  def verify_schema_version(db, query_options \\ []) do
    with {:ok, version} <- schema_version(db, query_options) do
      if version == @supported_schema_version do
        :ok
      else
        {:error,
         Error.new(:schema_incompatible, "unsupported Absurd schema version",
           operation: :verify_schema_version,
           metadata: %{expected: @supported_schema_version, actual: version}
         )}
      end
    end
  end

  @doc """
  Creates a queue idempotently.

  `:storage_mode` is `:unpartitioned` by default and may instead be
  `:partitioned`. Queue policy is configured separately with
  `set_queue_policy/4`.
  """
  @spec create_queue(Absurd.Client.queryable(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_queue(db, queue, options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, options} <- validate_keywords(options, @queue_create_defaults, :create_queue),
         {:ok, storage_mode} <- normalize_storage_mode(options[:storage_mode]),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, _result} <- create_queue_query(db, queue, storage_mode, query_options) do
      :ok
    end
  end

  @doc "Lists queue names in database order."
  @spec list_queues(Absurd.Client.queryable(), query_options()) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  def list_queues(db, query_options \\ []) do
    with {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <-
           query(
             db,
             "SELECT queue_name FROM absurd.list_queues()",
             [],
             query_options,
             :list_queues
           ) do
      shape_queue_names(result.rows)
    end
  end

  @doc "Returns a queue policy, or `nil` when the queue does not exist."
  @spec get_queue_policy(Absurd.Client.queryable(), String.t(), query_options()) ::
          {:ok, QueuePolicy.t() | nil} | {:error, Error.t()}
  def get_queue_policy(db, queue, query_options \\ []) do
    statement = """
    SELECT
      queue_name,
      storage_mode,
      partition_lookahead::text,
      partition_lookback::text,
      cleanup_ttl::text,
      cleanup_limit,
      detach_mode,
      detach_min_age::text
    FROM absurd.get_queue_policy($1)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <- query(db, statement, [queue], query_options, :get_queue_policy) do
      shape_queue_policy(result.rows)
    end
  end

  @doc """
  Updates the official SDK queue-policy fields.

  Interval values are PostgreSQL interval strings. Supported fields are
  `:partition_lookahead`, `:partition_lookback`, `:cleanup_ttl`,
  `:cleanup_limit`, `:detach_mode`, and `:detach_min_age`.
  """
  @spec set_queue_policy(Absurd.Client.queryable(), String.t(), keyword(), query_options()) ::
          :ok | {:error, Error.t()}
  def set_queue_policy(db, queue, policy, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, policy} <- normalize_queue_policy(policy),
         {:ok, query_options} <- validate_query_options(query_options),
         :ok <- JSON.validate(policy),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.set_queue_policy($1, $2::jsonb)",
             [queue, policy],
             query_options,
             :set_queue_policy
           ) do
      :ok
    end
  end

  @doc "Drops a queue if it exists."
  @spec drop_queue(Absurd.Client.queryable(), String.t(), query_options()) ::
          :ok | {:error, Error.t()}
  def drop_queue(db, queue, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.drop_queue($1)",
             [queue],
             query_options,
             :drop_queue
           ) do
      :ok
    end
  end

  @doc """
  Spawns a task through `absurd.spawn_task/4`.

  Parameters may be any `t:Absurd.JSON.value/0`. Options are validated and
  normalized to the upstream snake-case JSON object; Postgrex performs the
  single JSON encoding step.
  """
  @spec spawn_task(
          Absurd.Client.queryable(),
          String.t(),
          String.t(),
          JSON.value(),
          [spawn_option()]
        ) :: {:ok, SpawnResult.t()} | {:error, Error.t()}
  def spawn_task(db, queue, task_name, params, options \\ []) do
    statement = """
    SELECT task_id, run_id, attempt, created
    FROM absurd.spawn_task($1, $2, $3::jsonb, $4::jsonb)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, task_name} <- Name.validate_durable(task_name, :task),
         :ok <- JSON.validate(params),
         {:ok, options} <- normalize_spawn_options(options),
         {query_options, options} <- Map.pop(options, :query_options),
         :ok <- JSON.validate(options),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, task_name, params, options],
             query_options,
             :spawn_task
           ) do
      shape_spawn_result(result.rows, queue, :spawn_task)
    end
  end

  @doc "Returns the current task snapshot, or `nil` when no task exists."
  @spec get_task_result(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          query_options()
        ) :: {:ok, TaskResult.t() | nil} | {:error, Error.t()}
  def get_task_result(db, queue, task_id, query_options \\ []) do
    statement = """
    SELECT state, result, failure_reason
    FROM absurd.get_task_result($1, $2)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <-
           query(db, statement, [queue, task_id], query_options, :get_task_result) do
      shape_task_result(result.rows)
    end
  end

  @doc """
  Retries a failed task in place or creates a new logical task.

  Supported options are `:max_attempts`, `:spawn_new`, and `:query_options`.
  The returned `Absurd.SpawnResult.created` distinguishes the two modes.
  """
  @spec retry_task(Absurd.Client.queryable(), String.t(), binary(), keyword()) ::
          {:ok, SpawnResult.t()} | {:error, Error.t()}
  def retry_task(db, queue, task_id, options \\ []) do
    statement = """
    SELECT task_id, run_id, attempt, created
    FROM absurd.retry_task($1, $2, $3::jsonb)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         {:ok, options} <- normalize_retry_options(options),
         {query_options, options} <- Map.pop(options, :query_options),
         :ok <- JSON.validate(options),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, task_id, options],
             query_options,
             :retry_task
           ) do
      shape_spawn_result(result.rows, queue, :retry_task)
    end
  end

  @doc "Cancels a task without attempting to stop arbitrary external effects."
  @spec cancel_task(Absurd.Client.queryable(), String.t(), binary(), query_options()) ::
          :ok | {:error, Error.t()}
  def cancel_task(db, queue, task_id, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.cancel_task($1, $2)",
             [queue, task_id],
             query_options,
             :cancel_task
           ) do
      :ok
    end
  end

  @doc "Emits a first-write-wins event payload on a queue."
  @spec emit_event(
          Absurd.Client.queryable(),
          String.t(),
          String.t(),
          JSON.value(),
          query_options()
        ) :: :ok | {:error, Error.t()}
  def emit_event(db, queue, event_name, payload, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, event_name} <- Name.validate_durable(event_name, :event),
         :ok <- JSON.validate(payload),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.emit_event($1, $2, $3::jsonb)",
             [queue, event_name, payload],
             query_options,
             :emit_event
           ) do
      :ok
    end
  end

  @doc "Claims up to `:batch_size` runs for a worker."
  @spec claim_tasks(Absurd.Client.queryable(), String.t(), String.t(), keyword()) ::
          {:ok, [ClaimedTask.t()]} | {:error, Error.t()}
  def claim_tasks(db, queue, worker_id, options \\ []) do
    statement = """
    SELECT
      run_id,
      task_id,
      attempt,
      task_name,
      params,
      retry_strategy,
      max_attempts,
      headers,
      wake_event,
      event_payload
    FROM absurd.claim_task($1, $2, $3, $4)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_worker_id(worker_id),
         {:ok, options} <- validate_keywords(options, @claim_option_defaults, :claim_tasks),
         :ok <- validate_positive(options[:claim_timeout], :claim_timeout, :claim_tasks),
         :ok <- validate_positive(options[:batch_size], :batch_size, :claim_tasks),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         claim_seconds <- milliseconds_to_seconds(options[:claim_timeout]),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, worker_id, claim_seconds, options[:batch_size]],
             query_options,
             :claim_tasks
           ) do
      shape_claimed_tasks(result.rows)
    end
  end

  @doc "Completes a running run with a JSON result."
  @spec complete_run(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          JSON.value(),
          query_options()
        ) :: :ok | {:error, Error.t()}
  def complete_run(db, queue, run_id, result, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(run_id, :run_id),
         :ok <- JSON.validate(result),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.complete_run($1, $2, $3::jsonb)",
             [queue, run_id, result],
             query_options,
             :complete_run
           ) do
      :ok
    end
  end

  @doc """
  Schedules a running run at an absolute UTC time and releases its claim.

  Prefer `schedule_run_after/5` when a relative duration should use the
  database clock.
  """
  @spec schedule_run(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          DateTime.t(),
          query_options()
        ) :: :ok | {:error, Error.t()}
  def schedule_run(db, queue, run_id, wake_at, query_options \\ [])

  def schedule_run(db, queue, run_id, %DateTime{} = wake_at, query_options) do
    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(run_id, :run_id),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.schedule_run($1, $2, $3)",
             [queue, run_id, wake_at],
             query_options,
             :schedule_run
           ) do
      :ok
    end
  end

  def schedule_run(_db, _queue, _run_id, _wake_at, _query_options) do
    validation_error(:schedule_run, "wake_at must be a DateTime")
  end

  @doc "Schedules a run relative to the Absurd database clock."
  @spec schedule_run_after(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          non_neg_integer(),
          query_options()
        ) :: :ok | {:error, Error.t()}
  def schedule_run_after(db, queue, run_id, duration, query_options \\ []) do
    # Compute the wake time beside the durable state. Using the database clock
    # avoids application-node skew changing the requested relative delay.
    statement = """
    SELECT absurd.schedule_run(
      $1,
      $2,
      absurd.current_time() + make_interval(secs => $3::double precision)
    )
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(run_id, :run_id),
         :ok <- validate_non_negative(duration, :duration, :schedule_run_after),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             statement,
             [queue, run_id, duration / 1_000],
             query_options,
             :schedule_run_after
           ) do
      :ok
    end
  end

  @doc "Fails a run and lets the upstream retry policy choose its next state."
  @spec fail_run(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          JSON.value(),
          keyword()
        ) :: :ok | {:error, Error.t()}
  def fail_run(db, queue, run_id, reason, options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(run_id, :run_id),
         :ok <- JSON.validate(reason),
         {:ok, options} <- validate_keywords(options, @failure_option_defaults, :fail_run),
         :ok <- validate_optional_datetime(options[:retry_at], :retry_at, :fail_run),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.fail_run($1, $2, $3::jsonb, $4)",
             [queue, run_id, reason, options[:retry_at]],
             query_options,
             :fail_run
           ) do
      :ok
    end
  end

  @doc "Persists a task checkpoint and optionally extends its run claim."
  @spec set_task_checkpoint_state(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          String.t(),
          JSON.value(),
          binary(),
          keyword()
        ) :: :ok | {:error, Error.t()}
  def set_task_checkpoint_state(db, queue, task_id, step_name, state, owner_run_id, options \\ []) do
    # The upstream function verifies claim ownership, persists the checkpoint,
    # and optionally extends the lease as one operation. Splitting those actions
    # client-side would leave a committed effect paired with an expired claim.
    statement = """
    SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4::jsonb, $5, $6)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         {:ok, step_name} <- Name.validate_durable(step_name, :step),
         :ok <- JSON.validate(state),
         :ok <- validate_identifier(owner_run_id, :run_id),
         {:ok, options} <-
           validate_keywords(options, @checkpoint_option_defaults, :set_task_checkpoint_state),
         :ok <-
           validate_positive_optional(
             options[:extend_claim_by],
             :extend_claim_by,
             :set_task_checkpoint_state
           ),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         extend_seconds <- optional_milliseconds_to_seconds(options[:extend_claim_by]),
         {:ok, _result} <-
           query(
             db,
             statement,
             [queue, task_id, step_name, state, owner_run_id, extend_seconds],
             query_options,
             :set_task_checkpoint_state
           ) do
      :ok
    end
  end

  @doc "Extends a running claim by a positive millisecond duration."
  @spec extend_claim(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          pos_integer(),
          query_options()
        ) :: :ok | {:error, Error.t()}
  def extend_claim(db, queue, run_id, duration, query_options \\ []) do
    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(run_id, :run_id),
         :ok <- validate_positive(duration, :duration, :extend_claim),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, _result} <-
           query(
             db,
             "SELECT absurd.extend_claim($1, $2, $3)",
             [queue, run_id, milliseconds_to_seconds(duration)],
             query_options,
             :extend_claim
           ) do
      :ok
    end
  end

  @doc "Returns one checkpoint, or `nil` when it is not visible."
  @spec get_task_checkpoint_state(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          String.t(),
          keyword()
        ) :: {:ok, Checkpoint.t() | nil} | {:error, Error.t()}
  def get_task_checkpoint_state(db, queue, task_id, step_name, options \\ []) do
    statement = """
    SELECT checkpoint_name, state, status, owner_run_id, updated_at
    FROM absurd.get_task_checkpoint_state($1, $2, $3, $4)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         {:ok, step_name} <- Name.validate_durable(step_name, :step),
         {:ok, options} <-
           validate_keywords(options, @checkpoint_read_defaults, :get_task_checkpoint_state),
         :ok <-
           validate_boolean(
             options[:include_pending],
             :include_pending,
             :get_task_checkpoint_state
           ),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, task_id, step_name, options[:include_pending]],
             query_options,
             :get_task_checkpoint_state
           ) do
      shape_checkpoint(result.rows, :get_task_checkpoint_state)
    end
  end

  @doc "Returns committed checkpoints visible to the supplied run attempt."
  @spec get_task_checkpoint_states(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          binary(),
          query_options()
        ) :: {:ok, [Checkpoint.t()]} | {:error, Error.t()}
  def get_task_checkpoint_states(db, queue, task_id, run_id, query_options \\ []) do
    statement = """
    SELECT checkpoint_name, state, status, owner_run_id, updated_at
    FROM absurd.get_task_checkpoint_states($1, $2, $3)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         :ok <- validate_identifier(run_id, :run_id),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, task_id, run_id],
             query_options,
             :get_task_checkpoint_states
           ) do
      shape_checkpoints(result.rows)
    end
  end

  @doc "Atomically resolves or durably suspends an event wait."
  @spec await_event(
          Absurd.Client.queryable(),
          String.t(),
          binary(),
          binary(),
          String.t(),
          String.t(),
          keyword()
        ) :: {:ok, EventWait.t()} | {:error, Error.t()}
  def await_event(db, queue, task_id, run_id, step_name, event_name, options \\ []) do
    statement = """
    SELECT should_suspend, payload
    FROM absurd.await_event($1, $2, $3, $4, $5, $6)
    """

    with {:ok, queue} <- Name.validate_queue(queue),
         :ok <- validate_identifier(task_id, :task_id),
         :ok <- validate_identifier(run_id, :run_id),
         {:ok, step_name} <- Name.validate_durable(step_name, :step),
         {:ok, event_name} <- Name.validate_durable(event_name, :event),
         {:ok, options} <- validate_keywords(options, @event_wait_defaults, :await_event),
         {:ok, timeout} <- normalize_optional_timeout(options[:timeout], :await_event),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, result} <-
           query(
             db,
             statement,
             [queue, task_id, run_id, step_name, event_name, timeout],
             query_options,
             :await_event
           ) do
      shape_event_wait(result.rows)
    end
  end

  @doc "Runs one policy-driven cleanup batch for all queues or one selected queue."
  @spec cleanup_all_queues(Absurd.Client.queryable(), String.t() | nil, query_options()) ::
          {:ok, [CleanupResult.t()]} | {:error, Error.t()}
  def cleanup_all_queues(db, queue \\ nil, query_options \\ []) do
    with {:ok, queue} <- validate_optional_queue(queue),
         {:ok, query_options} <- validate_query_options(query_options),
         {:ok, result} <-
           query(
             db,
             "SELECT queue_name, tasks_deleted, events_deleted FROM absurd.cleanup_all_queues($1)",
             [queue],
             query_options,
             :cleanup_all_queues
           ) do
      shape_cleanup_results(result.rows)
    end
  end

  @doc "Deletes one bounded batch of terminal tasks older than `:ttl` milliseconds."
  @spec cleanup_tasks(Absurd.Client.queryable(), String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def cleanup_tasks(db, queue, options) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, options} <- validate_cleanup_options(options, :cleanup_tasks),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, result} <-
           query(
             db,
             "SELECT absurd.cleanup_tasks($1, $2, $3)",
             [queue, milliseconds_to_seconds(options[:ttl]), options[:limit]],
             query_options,
             :cleanup_tasks
           ) do
      shape_count(result.rows, :cleanup_tasks)
    end
  end

  @doc "Deletes one bounded batch of events older than `:ttl` milliseconds."
  @spec cleanup_events(Absurd.Client.queryable(), String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def cleanup_events(db, queue, options) do
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, options} <- validate_cleanup_options(options, :cleanup_events),
         {:ok, query_options} <- validate_query_options(options[:query_options]),
         {:ok, result} <-
           query(
             db,
             "SELECT absurd.cleanup_events($1, $2, $3)",
             [queue, milliseconds_to_seconds(options[:ttl]), options[:limit]],
             query_options,
             :cleanup_events
           ) do
      shape_count(result.rows, :cleanup_events)
    end
  end

  defp create_queue_query(db, queue, :unpartitioned, query_options) do
    query(
      db,
      "SELECT absurd.create_queue($1)",
      [queue],
      query_options,
      :create_queue
    )
  end

  defp create_queue_query(db, queue, :partitioned, query_options) do
    query(
      db,
      "SELECT absurd.create_queue($1, $2)",
      [queue, "partitioned"],
      query_options,
      :create_queue
    )
  end

  defp query(db, statement, params, query_options, operation) do
    # All SQL crosses this one observation and error-classification boundary.
    # Deliberately do not retry here: for a mutating call, losing the response can
    # mean the write committed, so an automatic replay may duplicate intent.
    Telemetry.span(:sql, operation, telemetry_metadata(operation, params), fn ->
      case Postgrex.query(db, statement, params, query_options) do
        {:ok, result} ->
          {:ok, result}

        {:error, exception} ->
          {:error,
           Error.from_exception(exception, operation,
             ambiguous?: operation in @mutating_operations
           )}
      end
    end)
  end

  defp telemetry_metadata(operation, [queue | _params]) when is_binary(queue) do
    %{operation: operation, queue: queue}
  end

  defp telemetry_metadata(operation, _params), do: %{operation: operation}

  defp validate_keywords(options, defaults, operation) when is_list(options) do
    if Keyword.keyword?(options) do
      case Keyword.validate(options, defaults) do
        {:ok, validated} -> {:ok, validated}
        {:error, unknown} -> validation_error(operation, "unknown options", %{options: unknown})
      end
    else
      validation_error(operation, "options must be a keyword list")
    end
  end

  defp validate_keywords(_options, _defaults, operation) do
    validation_error(operation, "options must be a keyword list")
  end

  defp validate_query_options(options) when is_list(options) do
    if Keyword.keyword?(options) do
      {:ok, options}
    else
      validation_error(:query, "query options must be a keyword list")
    end
  end

  defp validate_query_options(_options) do
    validation_error(:query, "query options must be a keyword list")
  end

  defp validate_worker_id(worker_id) when is_binary(worker_id) and byte_size(worker_id) > 0,
    do: :ok

  defp validate_worker_id(_worker_id) do
    validation_error(:claim_tasks, "worker_id must be a non-empty string")
  end

  defp validate_positive(value, _field, _operation) when is_integer(value) and value > 0, do: :ok

  defp validate_positive(_value, field, operation) do
    validation_error(operation, "#{field} must be a positive integer")
  end

  defp validate_non_negative(value, _field, _operation)
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_non_negative(_value, field, operation) do
    validation_error(operation, "#{field} must be a non-negative integer")
  end

  defp validate_optional_datetime(nil, _field, _operation), do: :ok
  defp validate_optional_datetime(%DateTime{}, _field, _operation), do: :ok

  defp validate_optional_datetime(_value, field, operation) do
    validation_error(operation, "#{field} must be a DateTime or nil")
  end

  defp normalize_optional_timeout(:infinity, _operation), do: {:ok, nil}

  defp normalize_optional_timeout(timeout, _operation)
       when is_integer(timeout) and timeout >= 0,
       do: {:ok, milliseconds_to_seconds(timeout)}

  defp normalize_optional_timeout(_timeout, operation) do
    validation_error(operation, "timeout must be a non-negative integer or :infinity")
  end

  defp validate_optional_queue(nil), do: {:ok, nil}
  defp validate_optional_queue(queue), do: Name.validate_queue(queue)

  defp validate_cleanup_options(options, operation) do
    with {:ok, options} <- validate_keywords(options, @cleanup_defaults, operation),
         :ok <- validate_non_negative(options[:ttl], :ttl, operation),
         :ok <- validate_positive(options[:limit], :limit, operation) do
      {:ok, options}
    end
  end

  # Schema durations are whole seconds. Always round up: rounding a positive
  # lease or timeout down could make it expire earlier than the caller requested.
  defp milliseconds_to_seconds(milliseconds), do: div(milliseconds + 999, 1_000)
  defp optional_milliseconds_to_seconds(nil), do: nil
  defp optional_milliseconds_to_seconds(milliseconds), do: milliseconds_to_seconds(milliseconds)

  defp normalize_storage_mode(mode) when mode in [:unpartitioned, "unpartitioned"],
    do: {:ok, :unpartitioned}

  defp normalize_storage_mode(mode) when mode in [:partitioned, "partitioned"],
    do: {:ok, :partitioned}

  defp normalize_storage_mode(_mode) do
    validation_error(:create_queue, "storage_mode must be :unpartitioned or :partitioned")
  end

  defp normalize_queue_policy(policy) do
    with {:ok, policy} <- validate_keywords(policy, @queue_policy_defaults, :set_queue_policy),
         :ok <- validate_intervals(policy),
         :ok <- validate_cleanup_limit(policy[:cleanup_limit]),
         {:ok, detach_mode} <- normalize_optional_detach_mode(policy[:detach_mode]) do
      normalized =
        policy
        |> Keyword.put(:detach_mode, detach_mode)
        # Omitted fields must stay omitted so the database can retain existing
        # policy values; sending JSON null would carry different update intent.
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

      {:ok, normalized}
    end
  end

  defp validate_intervals(policy) do
    fields = [:partition_lookahead, :partition_lookback, :cleanup_ttl, :detach_min_age]

    case Enum.find(fields, fn field ->
           value = policy[field]
           not (is_nil(value) or (is_binary(value) and String.trim(value) != ""))
         end) do
      nil -> :ok
      field -> validation_error(:set_queue_policy, "#{field} must be an interval string")
    end
  end

  defp validate_cleanup_limit(nil), do: :ok
  defp validate_cleanup_limit(value) when is_integer(value) and value > 0, do: :ok

  defp validate_cleanup_limit(_value) do
    validation_error(:set_queue_policy, "cleanup_limit must be a positive integer")
  end

  defp normalize_optional_detach_mode(nil), do: {:ok, nil}
  defp normalize_optional_detach_mode(mode) when mode in [:none, "none"], do: {:ok, "none"}
  defp normalize_optional_detach_mode(mode) when mode in [:empty, "empty"], do: {:ok, "empty"}

  defp normalize_optional_detach_mode(_mode) do
    validation_error(:set_queue_policy, "detach_mode must be :none or :empty")
  end

  defp normalize_spawn_options(options) do
    # Keep Postgrex transport options outside the JSON document and translate
    # public atom keys to the schema's stable snake-case wire representation.
    with {:ok, options} <- validate_keywords(options, @spawn_option_defaults, :spawn_task),
         :ok <- validate_positive_optional(options[:max_attempts], :max_attempts, :spawn_task),
         {:ok, retry_strategy} <- normalize_retry_strategy(options[:retry_strategy]),
         {:ok, headers} <- normalize_headers(options[:headers]),
         {:ok, cancellation} <- normalize_cancellation(options[:cancellation]),
         :ok <- validate_idempotency_key(options[:idempotency_key]),
         {:ok, query_options} <- validate_query_options(options[:query_options]) do
      persisted =
        [
          max_attempts: options[:max_attempts],
          retry_strategy: retry_strategy,
          headers: headers,
          cancellation: cancellation,
          idempotency_key: options[:idempotency_key]
        ]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
        |> Map.put(:query_options, query_options)

      {:ok, persisted}
    end
  end

  defp normalize_retry_options(options) do
    with {:ok, options} <- validate_keywords(options, @retry_option_defaults, :retry_task),
         :ok <- validate_positive_optional(options[:max_attempts], :max_attempts, :retry_task),
         :ok <- validate_boolean(options[:spawn_new], :spawn_new, :retry_task),
         {:ok, query_options} <- validate_query_options(options[:query_options]) do
      persisted =
        %{"spawn_new" => options[:spawn_new]}
        |> put_if_present("max_attempts", options[:max_attempts])
        |> Map.put(:query_options, query_options)

      {:ok, persisted}
    end
  end

  defp normalize_retry_strategy(nil), do: {:ok, nil}

  defp normalize_retry_strategy(strategy) do
    defaults = [kind: nil, base_seconds: nil, factor: nil, max_seconds: nil]

    with {:ok, strategy} <- validate_keywords(strategy, defaults, :retry_strategy),
         {:ok, kind} <- normalize_retry_kind(strategy[:kind]),
         :ok <- validate_retry_number(strategy[:base_seconds], :base_seconds, 86_400),
         :ok <- validate_retry_number(strategy[:factor], :factor, :infinity),
         :ok <- validate_retry_number(strategy[:max_seconds], :max_seconds, 86_400) do
      normalized =
        strategy
        |> Keyword.put(:kind, kind)
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

      {:ok, normalized}
    end
  end

  defp normalize_retry_kind(kind) when kind in [:none, "none"], do: {:ok, "none"}
  defp normalize_retry_kind(kind) when kind in [:fixed, "fixed"], do: {:ok, "fixed"}

  defp normalize_retry_kind(kind) when kind in [:exponential, "exponential"],
    do: {:ok, "exponential"}

  defp normalize_retry_kind(_kind) do
    validation_error(
      :retry_strategy,
      "retry strategy kind must be :none, :fixed, or :exponential"
    )
  end

  defp validate_retry_number(nil, _field, _maximum), do: :ok

  defp validate_retry_number(value, _field, :infinity) when is_number(value) and value >= 0,
    do: validate_encodable_number(value)

  defp validate_retry_number(value, _field, maximum)
       when is_number(value) and value >= 0 and value <= maximum,
       do: validate_encodable_number(value)

  defp validate_retry_number(_value, field, maximum) do
    suffix = if maximum == :infinity, do: "non-negative", else: "between 0 and #{maximum}"
    validation_error(:retry_strategy, "#{field} must be #{suffix}")
  end

  defp validate_encodable_number(value) do
    case JSON.validate(value) do
      :ok -> :ok
      {:error, _error} -> validation_error(:retry_strategy, "retry values must be finite numbers")
    end
  end

  defp normalize_headers(nil), do: {:ok, nil}

  defp normalize_headers(headers) when is_map(headers) and not is_struct(headers) do
    case JSON.validate(headers) do
      :ok -> {:ok, headers}
      {:error, error} -> {:error, error}
    end
  end

  defp normalize_headers(_headers) do
    validation_error(:spawn_task, "headers must be a JSON object")
  end

  defp normalize_cancellation(nil), do: {:ok, nil}

  defp normalize_cancellation(policy) do
    defaults = [max_duration: nil, max_delay: nil]

    with {:ok, policy} <- validate_keywords(policy, defaults, :cancellation),
         :ok <-
           validate_non_negative_optional(policy[:max_duration], :max_duration, :cancellation),
         :ok <- validate_non_negative_optional(policy[:max_delay], :max_delay, :cancellation) do
      normalized =
        policy
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

      # An empty policy means "use no override," not an object that replaces
      # database defaults with missing fields.
      if map_size(normalized) == 0, do: {:ok, nil}, else: {:ok, normalized}
    end
  end

  defp validate_positive_optional(nil, _field, _operation), do: :ok

  defp validate_positive_optional(value, _field, _operation)
       when is_integer(value) and value > 0,
       do: :ok

  defp validate_positive_optional(_value, field, operation) do
    validation_error(operation, "#{field} must be a positive integer")
  end

  defp validate_non_negative_optional(nil, _field, _operation), do: :ok

  defp validate_non_negative_optional(value, _field, _operation)
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_non_negative_optional(_value, field, operation) do
    validation_error(operation, "#{field} must be a non-negative integer")
  end

  defp validate_boolean(value, _field, _operation) when is_boolean(value), do: :ok

  defp validate_boolean(_value, field, operation) do
    validation_error(operation, "#{field} must be a boolean")
  end

  defp validate_idempotency_key(nil), do: :ok
  defp validate_idempotency_key(value) when is_binary(value), do: :ok

  defp validate_idempotency_key(_value) do
    validation_error(:spawn_task, "idempotency_key must be a string")
  end

  defp validate_identifier(value, _field) when is_binary(value) and byte_size(value) == 16,
    do: :ok

  defp validate_identifier(_value, field) do
    validation_error(:identifier, "#{field} must be a 16-byte UUID binary", %{field: field})
  end

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp shape_claimed_tasks(rows) do
    # Decode the entire result or reject it. A malformed row means the SDK/schema
    # contract is broken, so dispatching a seemingly valid prefix would hide that
    # protocol failure. Any undispatched claims remain recoverable by lease expiry.
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, tasks} ->
      case shape_claimed_task(row) do
        {:ok, task} -> {:cont, {:ok, [task | tasks]}}
        {:error, _error} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, tasks} -> {:ok, Enum.reverse(tasks)}
      {:error, _error} = error -> error
    end
  end

  defp shape_claimed_task([
         run_id,
         task_id,
         attempt,
         task_name,
         params,
         retry_strategy,
         max_attempts,
         headers,
         wake_event,
         event_payload
       ]) do
    {:ok,
     %ClaimedTask{
       run_id: run_id,
       task_id: task_id,
       attempt: attempt,
       task_name: task_name,
       params: params,
       retry_strategy: retry_strategy,
       max_attempts: max_attempts,
       headers: headers,
       wake_event: wake_event,
       event_payload: event_payload
     }}
  end

  defp shape_claimed_task(row), do: unexpected_rows(:claim_tasks, [row])

  defp shape_checkpoint([], _operation), do: {:ok, nil}

  defp shape_checkpoint([row], operation) do
    case shape_checkpoint_row(row) do
      {:ok, checkpoint} -> {:ok, checkpoint}
      {:error, _error} -> unexpected_rows(operation, [row])
    end
  end

  defp shape_checkpoint(rows, operation), do: unexpected_rows(operation, rows)

  defp shape_checkpoints(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, checkpoints} ->
      case shape_checkpoint_row(row) do
        {:ok, checkpoint} -> {:cont, {:ok, [checkpoint | checkpoints]}}
        {:error, _error} -> {:halt, unexpected_rows(:get_task_checkpoint_states, [row])}
      end
    end)
    |> case do
      {:ok, checkpoints} -> {:ok, Enum.reverse(checkpoints)}
      {:error, _error} = error -> error
    end
  end

  defp shape_checkpoint_row([checkpoint_name, state, status, owner_run_id, updated_at]) do
    {:ok,
     %Checkpoint{
       checkpoint_name: checkpoint_name,
       state: state,
       status: status,
       owner_run_id: owner_run_id,
       updated_at: updated_at
     }}
  end

  defp shape_checkpoint_row(_row), do: {:error, :invalid_checkpoint_row}

  defp shape_event_wait([[should_suspend, payload]]) when is_boolean(should_suspend) do
    {:ok, %EventWait{should_suspend: should_suspend, payload: payload}}
  end

  defp shape_event_wait(rows), do: unexpected_rows(:await_event, rows)

  defp shape_cleanup_results(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [queue_name, tasks_deleted, events_deleted], {:ok, results}
      when is_binary(queue_name) and is_integer(tasks_deleted) and is_integer(events_deleted) ->
        result = %CleanupResult{
          queue_name: queue_name,
          tasks_deleted: tasks_deleted,
          events_deleted: events_deleted
        }

        {:cont, {:ok, [result | results]}}

      row, _accumulator ->
        {:halt, unexpected_rows(:cleanup_all_queues, [row])}
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      {:error, _error} = error -> error
    end
  end

  defp shape_count([[count]], _operation) when is_integer(count) and count >= 0,
    do: {:ok, count}

  defp shape_count(rows, operation), do: unexpected_rows(operation, rows)

  defp shape_queue_names(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      [queue], {:ok, queues} when is_binary(queue) -> {:cont, {:ok, [queue | queues]}}
      row, _accumulator -> {:halt, unexpected_rows(:list_queues, [row])}
    end)
    |> case do
      {:ok, queues} -> {:ok, Enum.reverse(queues)}
      {:error, _error} = error -> error
    end
  end

  defp shape_queue_policy([]), do: {:ok, nil}

  defp shape_queue_policy([
         [
           queue_name,
           storage_mode,
           partition_lookahead,
           partition_lookback,
           cleanup_ttl,
           cleanup_limit,
           detach_mode,
           detach_min_age
         ]
       ]) do
    with {:ok, storage_mode} <- decode_storage_mode(storage_mode),
         {:ok, detach_mode} <- decode_detach_mode(detach_mode) do
      {:ok,
       %QueuePolicy{
         queue_name: queue_name,
         storage_mode: storage_mode,
         partition_lookahead: partition_lookahead,
         partition_lookback: partition_lookback,
         cleanup_ttl: cleanup_ttl,
         cleanup_limit: cleanup_limit,
         detach_mode: detach_mode,
         detach_min_age: detach_min_age
       }}
    end
  end

  defp shape_queue_policy(rows), do: unexpected_rows(:get_queue_policy, rows)

  defp shape_spawn_result([[task_id, run_id, attempt, created]], queue, _operation)
       when is_binary(task_id) and is_binary(run_id) and is_integer(attempt) and
              is_boolean(created) do
    {:ok,
     %SpawnResult{
       queue: queue,
       task_id: task_id,
       run_id: run_id,
       attempt: attempt,
       created: created
     }}
  end

  defp shape_spawn_result(rows, _queue, operation), do: unexpected_rows(operation, rows)

  defp shape_task_result([]), do: {:ok, nil}

  defp shape_task_result([[state, result, failure]]) do
    with {:ok, state} <- decode_task_state(state) do
      {:ok, %TaskResult{state: state, result: result, failure: failure}}
    end
  end

  defp shape_task_result(rows), do: unexpected_rows(:get_task_result, rows)

  defp decode_storage_mode("unpartitioned"), do: {:ok, :unpartitioned}
  defp decode_storage_mode("partitioned"), do: {:ok, :partitioned}
  defp decode_storage_mode(value), do: unexpected_value(:storage_mode, value)

  defp decode_detach_mode("none"), do: {:ok, :none}
  defp decode_detach_mode("empty"), do: {:ok, :empty}
  defp decode_detach_mode(value), do: unexpected_value(:detach_mode, value)

  defp decode_task_state("pending"), do: {:ok, :pending}
  defp decode_task_state("running"), do: {:ok, :running}
  defp decode_task_state("sleeping"), do: {:ok, :sleeping}
  defp decode_task_state("completed"), do: {:ok, :completed}
  defp decode_task_state("failed"), do: {:ok, :failed}
  defp decode_task_state("cancelled"), do: {:ok, :cancelled}
  defp decode_task_state(value), do: unexpected_value(:task_state, value)

  defp unexpected_rows(operation, rows) do
    # Row shapes are the SDK/schema protocol. Treat drift as a protocol error
    # instead of coercing surprising data into structs that look trustworthy.
    {:error,
     Error.new(:protocol, "unexpected rows returned by Absurd SQL",
       operation: operation,
       metadata: %{row_count: length(rows)}
     )}
  end

  defp unexpected_value(field, value) do
    {:error,
     Error.new(:protocol, "unexpected #{field} returned by Absurd SQL",
       metadata: %{field: field, value: value}
     )}
  end

  defp validation_error(operation, message, metadata \\ %{}) do
    {:error, Error.new(:validation, message, operation: operation, metadata: metadata)}
  end
end
