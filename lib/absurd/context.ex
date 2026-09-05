defmodule Absurd.Context do
  @moduledoc """
  Durable operations available to an `Absurd.Task` callback.

  A context carries the current queue, task and run identities, attempt,
  worker ID, and immutable headers. `new/4` preloads the checkpoints visible to
  the claimed run. A short-lived linked state process then tracks the local
  checkpoint cache and deterministic occurrence names for this execution only;
  PostgreSQL remains the durable authority.

  Contexts are normally created and closed by `Absurd.WorkerPool`. A custom
  executor must call `close/1` after the task callback finishes.

  Step callbacks use the same tagged result contract as tasks:

      Absurd.Context.step(context, "fetch-profile:v1", fn ->
        {:ok, %{"name" => "Ada"}}
      end)

  Use a decomposed step when an external system needs an idempotency key before
  the effect happens:

      with {:ok, step} <- Absurd.Context.begin_step(context, "charge:v1") do
        if step.done do
          {:ok, step.value}
        else
          key = Absurd.Context.idempotency_key(context, step)

          with {:ok, charge} <- Payments.charge(amount, idempotency_key: key) do
            Absurd.Context.complete_step(context, step, %{"charge_id" => charge.id})
          end
        end
      end

  Long work can extend its lease, while sleeps and waits release the worker
  slot durably:

      :ok = Absurd.Context.heartbeat(context, 60_000)
      :ok = Absurd.Context.sleep_for(context, "backoff:v1", 5_000)
      {:ok, payload} = Absurd.Context.await_event(context, "approved:order_123")
      {:ok, child} = Absurd.Context.await_task_result(context, spawned_child)

  Calls that durably suspend a run unwind the task callback through a private
  control signal. The worker consumes that signal without completing or
  failing the run.
  """

  alias Absurd.ClaimedTask
  alias Absurd.Error
  alias Absurd.EventWait
  alias Absurd.JSON
  alias Absurd.Name
  alias Absurd.SpawnResult
  alias Absurd.SQL
  alias Absurd.Step
  alias Absurd.TaskResult

  @default_claim_timeout 120_000
  @initial_poll_delay 50
  @maximum_poll_delay 1_000
  @minimum_heartbeat_interval 500
  @option_defaults [
    worker_id: "worker",
    claim_timeout: @default_claim_timeout,
    query_options: [],
    lease_notifier: nil
  ]
  @event_option_defaults [step_name: nil, timeout: :infinity]
  @child_option_defaults [queue: nil, step_name: nil, timeout: :infinity]

  @enforce_keys [
    :queue,
    :task_id,
    :run_id,
    :task_name,
    :attempt,
    :worker_id,
    :headers,
    :db,
    :claim_timeout,
    :query_options,
    :lease_notifier,
    :state,
    :control_ref
  ]
  defstruct @enforce_keys

  @typedoc "A notification invoked after the database extends the active lease."
  @type lease_notifier :: (pos_integer() -> any())

  @typedoc "The execution context passed to a task attempt."
  @type t :: %__MODULE__{
          queue: String.t(),
          task_id: binary(),
          run_id: binary(),
          task_name: String.t(),
          attempt: pos_integer(),
          worker_id: String.t(),
          headers: %{optional(String.t()) => JSON.value()},
          db: Absurd.Client.queryable(),
          claim_timeout: pos_integer(),
          query_options: keyword(),
          lease_notifier: lease_notifier(),
          state: pid(),
          control_ref: reference()
        }

  @doc """
  Builds an execution context from a claimed task and preloads its checkpoints.

  Options are:

    * `:worker_id` - non-empty worker identity, defaulting to `"worker"`;
    * `:claim_timeout` - active lease in milliseconds, defaulting to `120_000`;
    * `:query_options` - options passed to Postgrex queries;
    * `:lease_notifier` - callback invoked with the effective millisecond lease
      after a checkpoint or heartbeat extends it.

  Sub-second claim durations round up to a whole database second so the local
  lease observer never fires before the persisted lease.
  """
  @spec new(Absurd.Client.queryable(), String.t(), ClaimedTask.t(), keyword()) ::
          {:ok, t()} | {:error, Error.t()}
  def new(db, queue, %ClaimedTask{} = task, options \\ []) do
    # Load every checkpoint visible to this claimed run before user code starts.
    # The Agent built below is only an execution-local cache; this database read
    # is what makes a retried attempt resume from previously committed work.
    with {:ok, queue} <- Name.validate_queue(queue),
         {:ok, options} <- validate_options(options),
         :ok <- validate_db(db),
         :ok <- validate_worker_id(options[:worker_id]),
         :ok <- validate_positive_duration(options[:claim_timeout], :claim_timeout),
         :ok <- validate_query_options(options[:query_options]),
         {:ok, lease_notifier} <- normalize_lease_notifier(options[:lease_notifier]),
         {:ok, checkpoints} <-
           SQL.get_task_checkpoint_states(
             db,
             queue,
             task.task_id,
             task.run_id,
             options[:query_options]
           ),
         {:ok, state} <- start_state(checkpoints, task) do
      {:ok,
       %__MODULE__{
         queue: queue,
         task_id: task.task_id,
         run_id: task.run_id,
         task_name: task.task_name,
         attempt: task.attempt,
         worker_id: options[:worker_id],
         headers: task.headers || %{},
         db: db,
         claim_timeout: effective_duration(options[:claim_timeout]),
         query_options: options[:query_options],
         lease_notifier: lease_notifier,
         state: state,
         control_ref: make_ref()
       }}
    end
  end

  @doc """
  Stops the context's short-lived local state process.

  Closing a context never changes durable task state and is idempotent.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{state: state}) do
    if Process.alive?(state) do
      try do
        Agent.stop(state, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  @doc """
  Runs a checkpointed step or replays its previously committed value.

  The zero-arity callback must return `{:ok, json_value}` or
  `{:error, reason}`. Error results and raised exceptions are not checkpointed.
  """
  @spec step(t(), String.t(), (-> Absurd.Task.result())) :: Absurd.Task.result()
  def step(%__MODULE__{} = context, name, callback) when is_function(callback, 0) do
    # Allocate the occurrence before deciding whether to run. On replay the same
    # occurrence resolves to its committed value and user code is skipped.
    with {:ok, handle} <- begin_step(context, name) do
      if handle.done do
        {:ok, handle.value}
      else
        execute_step(context, handle, callback)
      end
    end
  end

  def step(%__MODULE__{}, _name, _callback) do
    validation_error(:step, "step callback must be a zero-arity function")
  end

  @doc """
  Allocates one deterministic checkpoint occurrence and returns its handle.

  Repeated uses of a logical name become `name`, `name#2`, and so on. A handle
  with `done: true` contains the committed value in `value`. Names that collide
  with an occurrence already allocated in this execution are rejected.
  """
  @spec begin_step(t(), String.t()) :: {:ok, Step.t()} | {:error, Error.t()}
  def begin_step(%__MODULE__{} = context, name) do
    with {:ok, name} <- Name.validate_durable(name, :step),
         {:ok, checkpoint_name} <- allocate_checkpoint_name(context, name),
         {:ok, checkpoint} <- lookup_checkpoint(context, checkpoint_name) do
      {:ok, step_handle(name, checkpoint_name, checkpoint)}
    end
  end

  @doc """
  Persists a value for a handle returned by `begin_step/2`.

  An already-completed handle returns its cached value without overwriting the
  checkpoint. New values must be JSON-compatible.
  """
  @spec complete_step(t(), Step.t(), JSON.value()) ::
          {:ok, JSON.value()} | {:error, Error.t()}
  def complete_step(%__MODULE__{}, %Step{done: true, value: value}, _new_value),
    do: {:ok, value}

  def complete_step(%__MODULE__{} = context, %Step{} = handle, value) do
    # Checkpoint persistence and lease extension happen in one database operation.
    # Update the local cache and watchdog only after that durable write succeeds.
    with {:ok, checkpoint_name} <- Name.validate_durable(handle.checkpoint_name, :step),
         :ok <- JSON.validate(value),
         :ok <-
           terminal_or_return(
             context,
             SQL.set_task_checkpoint_state(
               context.db,
               context.queue,
               context.task_id,
               checkpoint_name,
               value,
               context.run_id,
               extend_claim_by: context.claim_timeout,
               query_options: context.query_options
             )
           ) do
      cache_checkpoint(context, checkpoint_name, value)
      notify_lease(context, context.claim_timeout)
      {:ok, value}
    end
  end

  @doc """
  Derives a stable external idempotency key for a concrete step handle.

  The key combines the lowercase hexadecimal task UUID with the allocated
  checkpoint name. Calling this function does not allocate another occurrence.
  """
  @spec idempotency_key(t(), Step.t()) :: String.t()
  def idempotency_key(%__MODULE__{task_id: task_id}, %Step{checkpoint_name: checkpoint_name}) do
    Base.encode16(task_id, case: :lower) <> ":" <> checkpoint_name
  end

  @doc """
  Extends the current run's claim.

  `duration` is expressed in milliseconds and defaults to the original claim
  timeout. Successful extension also resets the worker's local lease observer.
  """
  @spec heartbeat(t(), pos_integer() | nil) :: :ok | {:error, Error.t()}
  def heartbeat(%__MODULE__{} = context, duration \\ nil) do
    duration = duration || context.claim_timeout

    with :ok <- validate_positive_duration(duration, :heartbeat),
         effective <- effective_duration(duration),
         :ok <-
           terminal_or_return(
             context,
             SQL.extend_claim(
               context.db,
               context.queue,
               context.run_id,
               effective,
               context.query_options
             )
           ) do
      notify_lease(context, effective)
      :ok
    end
  end

  @doc """
  Sleeps durably for a non-negative number of milliseconds.

  The chosen absolute wake time is checkpointed under `step_name`. If the wake
  time is still in the future, the run is scheduled using the database clock
  and the worker slot is released.
  """
  @spec sleep_for(t(), String.t(), non_neg_integer()) :: :ok | {:error, Error.t()}
  def sleep_for(%__MODULE__{} = context, step_name, duration) do
    with :ok <- validate_non_negative_duration(duration, :sleep_for) do
      wake_at = DateTime.add(DateTime.utc_now(), duration, :millisecond)
      sleep_until(context, step_name, wake_at)
    end
  end

  @doc """
  Sleeps durably until an absolute UTC `DateTime`.

  Replay always uses the checkpointed wake time rather than a newly supplied
  value. A past wake time returns immediately.
  """
  @spec sleep_until(t(), String.t(), DateTime.t()) :: :ok | {:error, Error.t()}
  def sleep_until(%__MODULE__{} = context, step_name, %DateTime{} = wake_at) do
    with {:ok, handle} <- begin_step(context, step_name),
         {:ok, wake_at} <- checkpoint_wake_at(context, handle, wake_at),
         remaining when is_integer(remaining) <-
           max(DateTime.diff(wake_at, DateTime.utc_now(), :millisecond), 0) do
      suspend_if_waiting(context, remaining)
    end
  end

  def sleep_until(%__MODULE__{}, _step_name, _wake_at) do
    validation_error(:sleep_until, "wake_at must be a DateTime")
  end

  @doc """
  Returns an event payload or durably suspends until the event or timeout.

  Options are `:step_name` and a millisecond `:timeout` (default `:infinity`).
  The default step name is `$awaitEvent:<event_name>`. An elapsed timeout
  returns an `Absurd.Error` with kind `:timeout` exactly once for an execution.
  """
  @spec await_event(t(), String.t(), keyword()) ::
          {:ok, JSON.value()} | {:error, Error.t()}
  def await_event(%__MODULE__{} = context, event_name, options \\ []) do
    with {:ok, event_name} <- Name.validate_durable(event_name, :event),
         {:ok, options} <- validate_keywords(options, @event_option_defaults, :await_event),
         :ok <- validate_timeout(options[:timeout], :await_event),
         {:ok, step_name} <- event_step_name(options[:step_name], event_name),
         {:ok, handle} <- begin_step(context, step_name) do
      await_event_handle(context, handle, event_name, options[:timeout])
    end
  end

  @doc """
  Polls a child task in another queue and checkpoints its terminal snapshot.

  `task` may be a 16-byte task ID or an `Absurd.SpawnResult`. Options are
  `:queue`, `:step_name`, and a millisecond `:timeout`. Same-queue waits are
  rejected before polling because they can deadlock worker capacity. Unknown
  children fail immediately.
  """
  @spec await_task_result(t(), SpawnResult.t() | binary(), keyword()) ::
          {:ok, TaskResult.t()} | {:error, Error.t()}
  def await_task_result(%__MODULE__{} = context, task, options \\ []) do
    with {:ok, options} <-
           validate_keywords(options, @child_option_defaults, :await_task_result),
         :ok <- validate_timeout(options[:timeout], :await_task_result),
         {:ok, queue, task_id} <- resolve_child(context, task, options[:queue]),
         :ok <- reject_current_queue(context.queue, queue),
         {:ok, step_name} <- child_step_name(options[:step_name], task_id),
         {:ok, handle} <- begin_step(context, step_name) do
      await_child_handle(context, handle, queue, task_id, options[:timeout])
    end
  end

  @doc "Emits a first-write-wins event payload on the current queue."
  @spec emit_event(t(), String.t(), JSON.value()) :: :ok | {:error, Error.t()}
  def emit_event(%__MODULE__{} = context, event_name, payload \\ nil) do
    terminal_or_return(
      context,
      SQL.emit_event(
        context.db,
        context.queue,
        event_name,
        payload,
        context.query_options
      )
    )
  end

  defp execute_step(context, handle, callback) do
    case callback.() do
      {:ok, value} ->
        complete_step(context, handle, value)

      # Failed callbacks deliberately leave no checkpoint. A later task attempt
      # must be allowed to execute the occurrence again.
      {:error, _reason} = error ->
        error

      other ->
        validation_error(:step, "step callback returned an invalid result", %{result: other})
    end
  end

  defp step_handle(name, checkpoint_name, {:found, value}) do
    %Step{name: name, checkpoint_name: checkpoint_name, done: true, value: value}
  end

  defp step_handle(name, checkpoint_name, :missing) do
    %Step{name: name, checkpoint_name: checkpoint_name, done: false, value: nil}
  end

  defp allocate_checkpoint_name(context, name) do
    # Occurrence numbers are deterministic only when task control flow is
    # deterministic. Retried executions must reach repeated names in the same
    # order for `name`, `name#2`, ... to identify the same durable effects.
    Agent.get_and_update(context.state, fn state ->
      count = Map.get(state.counters, name, 0) + 1
      checkpoint_name = if count == 1, do: name, else: "#{name}##{count}"

      if MapSet.member?(state.allocated_names, checkpoint_name) do
        {validation_error(:begin_step, "step name collides with an allocated occurrence", %{
           checkpoint_name: checkpoint_name
         }), state}
      else
        next = %{
          state
          | counters: Map.put(state.counters, name, count),
            allocated_names: MapSet.put(state.allocated_names, checkpoint_name)
        }

        {{:ok, checkpoint_name}, next}
      end
    end)
  end

  defp lookup_checkpoint(context, checkpoint_name) do
    # Prefer the preload/cache, but treat a cache miss as inconclusive. PostgreSQL
    # remains the authority for whether this run can see a committed checkpoint.
    case Agent.get(context.state, &Map.fetch(&1.checkpoints, checkpoint_name)) do
      {:ok, value} ->
        {:ok, {:found, value}}

      :error ->
        with {:ok, checkpoint} <-
               SQL.get_task_checkpoint_state(
                 context.db,
                 context.queue,
                 context.task_id,
                 checkpoint_name,
                 query_options: context.query_options
               ) do
          use_checkpoint(context, checkpoint_name, checkpoint)
        end
    end
  end

  defp use_checkpoint(_context, _checkpoint_name, nil), do: {:ok, :missing}

  defp use_checkpoint(context, checkpoint_name, checkpoint) do
    cache_and_return(context, checkpoint_name, checkpoint.state)
  end

  defp cache_and_return(context, checkpoint_name, value) do
    cache_checkpoint(context, checkpoint_name, value)
    {:ok, {:found, value}}
  end

  defp cache_checkpoint(context, checkpoint_name, value) do
    Agent.update(context.state, &put_in(&1, [:checkpoints, checkpoint_name], value))
  end

  defp checkpoint_wake_at(_context, %Step{done: true, value: value}, _wake_at),
    do: decode_wake_at(value)

  defp checkpoint_wake_at(context, %Step{} = handle, wake_at) do
    # Persist an absolute instant on first execution. Recomputing `now + duration`
    # after a retry would silently lengthen every interrupted sleep.
    encoded = DateTime.to_iso8601(wake_at)

    with {:ok, _encoded} <- complete_step(context, handle, encoded) do
      {:ok, wake_at}
    end
  end

  defp decode_wake_at(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, wake_at, _offset} ->
        {:ok, wake_at}

      {:error, _reason} ->
        protocol_error(:sleep_until, "sleep checkpoint is not an ISO 8601 time")
    end
  end

  defp decode_wake_at(_value) do
    protocol_error(:sleep_until, "sleep checkpoint must contain an ISO 8601 time")
  end

  defp suspend_if_waiting(_context, 0), do: :ok

  defp suspend_if_waiting(context, remaining) do
    # Commit the sleeping state before unwinding the callback. Once this succeeds
    # the current runner must not complete the run, so signal/2 transfers control
    # directly back to Runner.
    case terminal_or_return(
           context,
           SQL.schedule_run_after(
             context.db,
             context.queue,
             context.run_id,
             remaining,
             context.query_options
           )
         ) do
      :ok -> signal(context, :suspended)
      {:error, %Error{}} = error -> error
    end
  end

  defp await_event_handle(_context, %Step{done: true, value: value}, _event_name, _timeout),
    do: {:ok, value}

  defp await_event_handle(context, %Step{} = handle, event_name, timeout) do
    if consume_event_timeout(context, event_name) do
      {:error,
       Error.new(:timeout, "timed out waiting for event",
         operation: :await_event,
         metadata: %{event_name: event_name}
       )}
    else
      await_event_in_database(context, handle, event_name, timeout)
    end
  end

  defp await_event_in_database(context, handle, event_name, timeout) do
    # Resolution and suspension are one database transaction. That atomic boundary
    # closes the race where an event arrives between "not found" and "go to sleep."
    result =
      SQL.await_event(
        context.db,
        context.queue,
        context.task_id,
        context.run_id,
        handle.checkpoint_name,
        event_name,
        timeout: timeout,
        query_options: context.query_options
      )

    case terminal_or_return(context, result) do
      {:ok, %EventWait{should_suspend: false, payload: payload}} ->
        cache_checkpoint(context, handle.checkpoint_name, payload)
        clear_event_payload(context)
        {:ok, payload}

      {:ok, %EventWait{should_suspend: true}} ->
        signal(context, :suspended)

      {:error, %Error{}} = error ->
        error
    end
  end

  defp consume_event_timeout(context, event_name) do
    # A timed event wait is resumed as another claim. The claim carries the event
    # name with no payload, which is consumed once so a second call in this same
    # execution does not manufacture another timeout.
    Agent.get_and_update(context.state, fn state ->
      timed_out = state.wake_event == event_name and is_nil(state.event_payload)

      next_state =
        if timed_out do
          %{state | wake_event: nil, event_payload: nil}
        else
          state
        end

      {timed_out, next_state}
    end)
  end

  defp clear_event_payload(context) do
    Agent.update(context.state, &%{&1 | event_payload: nil})
  end

  defp await_child_handle(_context, %Step{done: true, value: value}, _queue, _task_id, _timeout) do
    decode_task_result(value)
  end

  defp await_child_handle(context, %Step{} = handle, queue, task_id, timeout) do
    # Child waits occupy this runner, unlike event waits. Heartbeat at half the
    # lease so a healthy parent cannot be reclaimed while it polls another queue.
    started_at = System.monotonic_time(:millisecond)
    heartbeat_interval = max(div(context.claim_timeout, 2), @minimum_heartbeat_interval)

    poll = %{
      handle: handle,
      queue: queue,
      task_id: task_id,
      timeout: timeout,
      started_at: started_at,
      delay: @initial_poll_delay,
      next_heartbeat: started_at + heartbeat_interval,
      heartbeat_interval: heartbeat_interval
    }

    poll_child(context, poll)
  end

  defp poll_child(context, poll) do
    case SQL.get_task_result(context.db, poll.queue, poll.task_id, context.query_options) do
      {:ok, nil} ->
        unknown_child_error(poll.queue, poll.task_id)

      {:ok, %TaskResult{} = result} ->
        poll_child_result(context, poll, result)

      {:error, %Error{}} = error ->
        error
    end
  end

  defp poll_child_result(context, poll, %TaskResult{} = result)
       when result.state in [:completed, :failed, :cancelled] do
    # Cache only terminal snapshots. Replaying a pending/running state would make
    # the parent permanently observe a result that was merely transient.
    encoded = encode_task_result(result)

    with {:ok, value} <- complete_step(context, poll.handle, encoded) do
      decode_task_result(value)
    end
  end

  defp poll_child_result(context, poll, _result) do
    # Exponential polling reduces database pressure while the cap preserves
    # reasonable completion latency. Timeout and lease clocks use monotonic time
    # so wall-clock adjustments cannot move their deadlines.
    with {:ok, remaining} <-
           remaining_timeout(poll.timeout, poll.started_at, :await_task_result),
         {:ok, next_heartbeat} <-
           maybe_heartbeat(context, poll.next_heartbeat, poll.heartbeat_interval) do
      sleep_for = if remaining == :infinity, do: poll.delay, else: min(poll.delay, remaining)
      Process.sleep(sleep_for)

      next_poll = %{
        poll
        | delay: min(poll.delay * 2, @maximum_poll_delay),
          next_heartbeat: next_heartbeat
      }

      poll_child(context, next_poll)
    end
  end

  defp maybe_heartbeat(context, next_heartbeat, heartbeat_interval) do
    now = System.monotonic_time(:millisecond)

    if now >= next_heartbeat do
      case heartbeat(context) do
        :ok -> {:ok, now + heartbeat_interval}
        {:error, %Error{}} = error -> error
      end
    else
      {:ok, next_heartbeat}
    end
  end

  defp remaining_timeout(:infinity, _started_at, _operation), do: {:ok, :infinity}

  defp remaining_timeout(timeout, started_at, operation) do
    elapsed = System.monotonic_time(:millisecond) - started_at
    remaining = max(timeout - elapsed, 0)

    if remaining == 0 do
      {:error, Error.new(:timeout, "timed out waiting for task result", operation: operation)}
    else
      {:ok, remaining}
    end
  end

  defp encode_task_result(%TaskResult{state: :completed, result: result}) do
    %{"state" => "completed", "result" => result}
  end

  defp encode_task_result(%TaskResult{state: :failed, failure: failure}) do
    %{"state" => "failed", "failure" => failure}
  end

  defp encode_task_result(%TaskResult{state: :cancelled}), do: %{"state" => "cancelled"}

  defp decode_task_result(%{"state" => "completed", "result" => result}) do
    {:ok, %TaskResult{state: :completed, result: result, failure: nil}}
  end

  defp decode_task_result(%{"state" => "failed", "failure" => failure}) do
    {:ok, %TaskResult{state: :failed, result: nil, failure: failure}}
  end

  defp decode_task_result(%{"state" => "cancelled"}) do
    {:ok, %TaskResult{state: :cancelled, result: nil, failure: nil}}
  end

  defp decode_task_result(_value) do
    protocol_error(:await_task_result, "child result checkpoint has an invalid shape")
  end

  defp resolve_child(_context, %SpawnResult{} = spawned, queue_override) do
    with {:ok, queue} <- Name.validate_queue(spawned.queue),
         :ok <- validate_child_id(spawned.task_id),
         :ok <- validate_child_queue_override(queue_override, queue) do
      {:ok, queue, spawned.task_id}
    end
  end

  defp resolve_child(context, task_id, queue)
       when is_binary(task_id) and byte_size(task_id) == 16 do
    with {:ok, queue} <- Name.validate_queue(queue || context.queue) do
      {:ok, queue, task_id}
    end
  end

  defp resolve_child(_context, _task, _queue) do
    validation_error(:await_task_result, "child must be a spawn result or 16-byte task ID")
  end

  defp validate_child_id(task_id) when is_binary(task_id) and byte_size(task_id) == 16, do: :ok

  defp validate_child_id(_task_id) do
    validation_error(:await_task_result, "child task ID must be a 16-byte UUID")
  end

  defp validate_child_queue_override(nil, _queue), do: :ok
  defp validate_child_queue_override(queue, queue), do: :ok

  defp validate_child_queue_override(_queue_override, queue) do
    {:error,
     Error.new(:configuration, "queue override does not match the child spawn result",
       operation: :await_task_result,
       metadata: %{queue: queue}
     )}
  end

  defp reject_current_queue(queue, queue) do
    # If all runners in one pool synchronously waited on children in that same
    # pool, no capacity would remain to execute those children.
    {:error,
     Error.new(:configuration, "a task cannot await another task in the same queue",
       operation: :await_task_result,
       metadata: %{queue: queue}
     )}
  end

  defp reject_current_queue(_current_queue, _child_queue), do: :ok

  defp child_step_name(nil, task_id) do
    {:ok, "$awaitTaskResult:" <> Base.encode16(task_id, case: :lower)}
  end

  defp child_step_name(step_name, _task_id), do: Name.validate_durable(step_name, :step)

  defp event_step_name(nil, event_name), do: {:ok, "$awaitEvent:" <> event_name}
  defp event_step_name(step_name, _event_name), do: Name.validate_durable(step_name, :step)

  defp unknown_child_error(queue, task_id) do
    {:error,
     Error.new(:unknown_task, "child task result was not found",
       operation: :await_task_result,
       metadata: %{queue: queue, task_id: task_id}
     )}
  end

  defp terminal_or_return(context, {:error, %Error{kind: kind}})
       when kind in [:cancelled, :failed_run] do
    # These errors mean PostgreSQL has already made the run terminal. Unwind user
    # code immediately so it cannot perform more effects or publish another state.
    signal(context, kind)
  end

  defp terminal_or_return(context, {:error, %Error{kind: :ambiguous} = error}) do
    # The run may already be sleeping. Stop callback effects and let the runner
    # report uncertainty without publishing a contradictory completion/failure.
    signal(context, {:ambiguous, error})
  end

  defp terminal_or_return(_context, result), do: result

  defp signal(context, reason) do
    # The unique reference authenticates this context's private control signal;
    # Runner will treat every other throw as a task failure.
    throw({:absurd_context_control, context.control_ref, reason})
  end

  defp notify_lease(context, duration) do
    # The durable extension has already succeeded before this callback runs. A
    # local observer failure must therefore not turn a committed operation into a
    # reported error or invite the caller to retry it.
    try do
      context.lease_notifier.(duration)
    rescue
      _exception -> :ok
    catch
      _kind, _reason -> :ok
    end

    :ok
  end

  defp start_state(checkpoints, task) do
    # This linked Agent is intentionally disposable. It removes repeated reads and
    # tracks occurrence order, but every value needed after a crash is in PostgreSQL.
    checkpoint_cache = Map.new(checkpoints, &{&1.checkpoint_name, &1.state})

    case Agent.start_link(fn ->
           %{
             checkpoints: checkpoint_cache,
             counters: %{},
             allocated_names: MapSet.new(),
             wake_event: task.wake_event,
             event_payload: task.event_payload
           }
         end) do
      {:ok, state} ->
        {:ok, state}

      {:error, reason} ->
        {:error,
         Error.new(:configuration, "could not start context state",
           operation: :new_context,
           cause: reason
         )}
    end
  end

  defp validate_options(options) do
    validate_keywords(options, @option_defaults, :new_context)
  end

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

  defp validate_db(nil), do: validation_error(:new_context, "db is required")
  defp validate_db(_db), do: :ok

  defp validate_worker_id(worker_id) when is_binary(worker_id) and byte_size(worker_id) > 0,
    do: :ok

  defp validate_worker_id(_worker_id) do
    validation_error(:new_context, "worker_id must be a non-empty string")
  end

  defp validate_query_options(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: :ok,
      else: validation_error(:new_context, "query_options must be a keyword list")
  end

  defp validate_query_options(_options) do
    validation_error(:new_context, "query_options must be a keyword list")
  end

  defp normalize_lease_notifier(nil), do: {:ok, fn _duration -> :ok end}
  defp normalize_lease_notifier(callback) when is_function(callback, 1), do: {:ok, callback}

  defp normalize_lease_notifier(_callback) do
    validation_error(:new_context, "lease_notifier must be a one-arity function")
  end

  defp validate_positive_duration(value, _field) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_duration(_value, field) do
    validation_error(field, "#{field} must be a positive integer in milliseconds")
  end

  defp validate_non_negative_duration(value, _operation)
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_non_negative_duration(_value, operation) do
    validation_error(operation, "duration must be a non-negative integer in milliseconds")
  end

  defp validate_timeout(:infinity, _operation), do: :ok
  defp validate_timeout(value, _operation) when is_integer(value) and value >= 0, do: :ok

  defp validate_timeout(_value, operation) do
    validation_error(operation, "timeout must be a non-negative integer or :infinity")
  end

  defp effective_duration(duration), do: div(duration + 999, 1_000) * 1_000

  defp validation_error(operation, message, metadata \\ %{}) do
    {:error, Error.new(:validation, message, operation: operation, metadata: metadata)}
  end

  defp protocol_error(operation, message) do
    {:error, Error.new(:protocol, message, operation: operation)}
  end
end
