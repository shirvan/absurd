defmodule Absurd.Runner do
  # A runner is a disposable process for exactly one database claim. It owns no
  # durable state: checkpoints, retries, and terminal status all live in
  # PostgreSQL, so killing this process only abandons a lease that can be claimed
  # again later.
  @moduledoc false

  use GenServer

  alias Absurd.Context
  alias Absurd.Error
  alias Absurd.Failure
  alias Absurd.LeaseWatchdog
  alias Absurd.SQL
  alias Absurd.TaskCatalog
  alias Absurd.Telemetry

  require Logger

  @unknown_task_base 15_000
  @unknown_task_jitter_slots 16

  # These callbacks are callable only through DynamicSupervisor child startup.
  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      # The database retry policy, not OTP restart intensity, decides whether a
      # failed attempt gets another run.
      restart: :temporary,
      shutdown: :brutal_kill
    }
  end

  @impl GenServer
  def init(options) do
    {:ok, Map.new(options), {:continue, :execute}}
  end

  @impl GenServer
  def handle_continue(:execute, state) do
    # Keep arbitrary user work out of init/1 so child startup establishes a live
    # runner promptly instead of blocking on the entire task callback.
    metadata = log_metadata(state)
    {:ok, watchdog} = LeaseWatchdog.start_link(self(), state.claim_timeout, metadata)

    try do
      Telemetry.span(:runner, :execute, telemetry_metadata(state), fn ->
        execute_claim(state, watchdog)
      end)
    after
      LeaseWatchdog.stop(watchdog)
    end

    {:stop, :normal, state}
  end

  defp execute_claim(state, watchdog) do
    case TaskCatalog.fetch(state.catalog, state.task.task_name) do
      # Durable task names can outlive one deployed code version. Missing code is
      # treated as a rollout mismatch, not immediately as a permanent task error.
      nil -> defer_unknown_task(state)
      task_module -> execute_known_task(state, watchdog, task_module)
    end
  end

  defp execute_known_task(state, watchdog, task_module) do
    context_options = [
      worker_id: state.worker_id,
      claim_timeout: state.claim_timeout,
      query_options: state.query_options,
      lease_notifier: &LeaseWatchdog.reset(watchdog, &1)
    ]

    case Context.new(state.db, state.queue, state.task, context_options) do
      {:ok, context} ->
        # Context owns its short-lived cache process. Always close it before the
        # runner publishes the terminal database transition.
        outcome = execute_task_callback(state, task_module, context)
        Context.close(context)
        finalize_outcome(state, outcome)

      {:error, %Error{} = error} ->
        fail_run(state, error, [])
    end
  end

  defp execute_task_callback(state, task_module, context) do
    control_ref = context.control_ref

    try do
      state.hooks
      |> invoke_task_hook(task_module, state.task.params, context)
      |> normalize_task_result()
    rescue
      exception -> {:failure, exception, __STACKTRACE__}
    catch
      # Context uses a private, reference-tagged throw for non-local control flow
      # such as durable sleep or cancellation. Matching the reference prevents a
      # task's unrelated throw from impersonating an internal control signal.
      :throw, {:absurd_context_control, ^control_ref, reason}
      when reason in [:suspended, :cancelled, :failed_run] ->
        {:control, reason}

      :throw, {:absurd_context_control, ^control_ref, {:ambiguous, %Error{} = error}} ->
        {:ambiguous, error}

      kind, reason ->
        {:failure, {kind, reason}, __STACKTRACE__}
    end
  end

  defp invoke_task_hook(nil, task_module, params, context) do
    task_module.run(params, context)
  end

  defp invoke_task_hook(hooks, task_module, params, context) do
    # Give the hook a continuation so it can bracket execution (for tracing,
    # timing, or context setup) without replacing the task dispatch contract.
    if function_exported?(hooks, :wrap_task_execution, 2) do
      hooks.wrap_task_execution(context, fn -> task_module.run(params, context) end)
    else
      task_module.run(params, context)
    end
  end

  defp normalize_task_result({:ok, value}), do: {:success, value}
  defp normalize_task_result({:error, reason}), do: {:failure, reason, []}

  defp normalize_task_result(other) do
    exception =
      RuntimeError.exception(
        "Absurd task returned #{inspect(other, limit: 20, printable_limit: 1_000)}; " <>
          "expected {:ok, value} or {:error, reason}"
      )

    {:failure, exception, []}
  end

  # Context has already committed these transitions in the database. Writing a
  # second terminal outcome here could overwrite cancellation or wake scheduling.
  defp finalize_outcome(_state, {:control, :suspended}), do: :suspended
  defp finalize_outcome(_state, {:control, :cancelled}), do: :cancelled
  defp finalize_outcome(_state, {:control, :failed_run}), do: :already_failed

  defp finalize_outcome(state, {:ambiguous, error}) do
    log_ambiguous(state, error.operation, error)
  end

  defp finalize_outcome(state, {:success, result}) do
    case SQL.complete_run(state.db, state.queue, state.task.run_id, result, state.query_options) do
      :ok ->
        :completed

      {:error, %Error{kind: :cancelled}} ->
        :cancelled

      {:error, %Error{kind: :failed_run}} ->
        :already_failed

      {:error, %Error{kind: :ambiguous} = error} ->
        # Retrying a write whose outcome is unknown can turn a successful commit
        # into a contradictory failure. Leave reconciliation to durable state.
        log_ambiguous(state, :complete_run, error)

      {:error, %Error{} = error} ->
        # A definitive rejection means completion did not commit. Record that as
        # an attempt failure so the database retry policy still gets the decision.
        fail_run(state, error, [])
    end
  end

  defp finalize_outcome(state, {:failure, %Error{kind: :ambiguous} = error, _stacktrace}) do
    # Also preserve ambiguous errors propagated or raised by application code
    # using the low-level SQL API rather than a context control signal.
    log_ambiguous(state, error.operation, error)
  end

  defp finalize_outcome(state, {:failure, reason, stacktrace}) do
    fail_run(state, reason, stacktrace)
  end

  defp fail_run(state, reason, stacktrace) do
    # Failure serialization is deliberately bounded before it crosses the
    # database boundary. The SQL function applies the durable retry policy and
    # chooses whether the task becomes pending again or terminally failed.
    failure = Failure.serialize(reason, stacktrace)
    options = [query_options: state.query_options]

    case SQL.fail_run(state.db, state.queue, state.task.run_id, failure, options) do
      :ok ->
        :failed

      {:error, %Error{kind: :cancelled}} ->
        :cancelled

      {:error, %Error{kind: :failed_run}} ->
        :already_failed

      {:error, %Error{kind: :ambiguous} = error} ->
        log_ambiguous(state, :fail_run, error)

      {:error, %Error{} = error} ->
        Logger.error(
          "Absurd could not finalize a failed task run",
          log_metadata(state) ++ [error_kind: error.kind]
        )

        :unfinalized
    end
  end

  defp defer_unknown_task(state) do
    # Deterministic per-run jitter prevents every worker from repeatedly claiming
    # an unavailable task at the same instant during a rolling deployment.
    delay = @unknown_task_base + unknown_task_jitter(state.task.run_id) * 1_000

    case SQL.schedule_run_after(
           state.db,
           state.queue,
           state.task.run_id,
           delay,
           state.query_options
         ) do
      :ok ->
        Logger.warning(
          "Absurd deferred a claimed task absent from this worker catalog",
          log_metadata(state) ++ [defer_milliseconds: delay]
        )

        :deferred

      {:error, %Error{kind: :cancelled}} ->
        :cancelled

      {:error, %Error{kind: :failed_run}} ->
        :already_failed

      {:error, %Error{kind: :ambiguous} = error} ->
        log_ambiguous(state, :defer_unknown_task, error)

      {:error, %Error{} = error} ->
        fail_run(state, error, [])
    end
  end

  defp log_ambiguous(state, operation, error) do
    # There is intentionally no compensating write here: a lost response does not
    # tell us whether PostgreSQL committed, and guessing would weaken correctness.
    Logger.error(
      "Absurd worker could not observe a durable write outcome",
      log_metadata(state) ++ [operation: operation, error_kind: error.kind]
    )

    :ambiguous
  end

  defp log_metadata(state) do
    [
      queue: state.queue,
      task_name: Telemetry.bounded_string(state.task.task_name, 128),
      task_id: Base.encode16(state.task.task_id, case: :lower),
      run_id: Base.encode16(state.task.run_id, case: :lower)
    ]
  end

  defp telemetry_metadata(state) do
    %{
      queue: state.queue,
      task_name: Telemetry.bounded_string(state.task.task_name, 128),
      task_id: Base.encode16(state.task.task_id, case: :lower),
      run_id: Base.encode16(state.task.run_id, case: :lower),
      attempt: state.task.attempt
    }
  end

  defp unknown_task_jitter(run_id) do
    # Use an explicit 32-bit FNV-1a hash rather than VM-local hashing so the same
    # durable UUID maps to the same delay bucket on every worker.
    hash =
      run_id
      |> canonical_uuid()
      |> :binary.bin_to_list()
      |> Enum.reduce(2_166_136_261, fn byte, hash ->
        hash
        |> Bitwise.bxor(byte)
        |> Kernel.*(16_777_619)
        |> Bitwise.band(0xFFFFFFFF)
      end)

    rem(hash, @unknown_task_jitter_slots)
  end

  defp canonical_uuid(run_id) do
    hex = Base.encode16(run_id, case: :lower)

    Enum.join(
      [
        binary_part(hex, 0, 8),
        binary_part(hex, 8, 4),
        binary_part(hex, 12, 4),
        binary_part(hex, 16, 4),
        binary_part(hex, 20, 12)
      ],
      "-"
    )
  end
end
