defmodule Absurd.Poller do
  # The poller is the worker pool's single capacity accountant. PostgreSQL owns
  # the durable work; this process only decides how many claims may be admitted
  # to this local pool.
  #
  # Invariant: `active` contains one monitor reference for every live runner.
  # Claims are limited to `concurrency - map_size(active)` so a pool cannot
  # oversubscribe itself even when polls and runner exits happen close together.
  @moduledoc false

  use GenServer

  alias Absurd.Error
  alias Absurd.Runner
  alias Absurd.SQL

  require Logger

  @maximum_error_backoff 30_000

  # These callbacks are callable only so Absurd.WorkerPool can supervise the process.
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
      restart: :permanent,
      shutdown: Keyword.fetch!(options, :shutdown) + 1_000
    }
  end

  @impl GenServer
  def init(options) do
    # Trapping exits routes supervisor shutdown through terminate/2. That lets us
    # stop polling first, then give already-running callbacks time to finish.
    Process.flag(:trap_exit, true)

    state =
      options
      |> Map.new()
      |> Map.merge(%{
        active: %{},
        error_attempt: 0,
        poll_timer: nil,
        poll_token: nil,
        runner_supervisor: nil
      })
      |> schedule_poll(0)

    {:ok, state}
  end

  @impl GenServer
  def handle_info({:poll, token}, %{poll_token: token} = state) do
    # Only the most recently scheduled poll may claim work. A cancelled timer can
    # already be in the mailbox, so matching the token is stronger than merely
    # cancelling its timer reference.
    state = %{state | poll_timer: nil, poll_token: nil}
    {:noreply, state |> ensure_runner_supervisor() |> claim_available_capacity()}
  end

  def handle_info({:poll, _stale_token}, state), do: {:noreply, state}

  def handle_info({:DOWN, reference, :process, _pid, _reason}, state) do
    if Map.has_key?(state.active, reference) do
      # Runner completion returns one unit of capacity. Poll immediately so a
      # busy queue does not wait for the ordinary polling interval.
      next_state = %{state | active: Map.delete(state.active, reference)}
      {:noreply, schedule_poll(next_state, 0)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:EXIT, _linked_process, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    # The poller does not kill runners here. It waits until they finish or the
    # pool's shutdown budget expires; the DynamicSupervisor is stopped next and
    # enforces the final teardown.
    cancel_timer(state.poll_timer)
    deadline = System.monotonic_time(:millisecond) + state.shutdown
    drain_runners(state.active, deadline)
  end

  defp ensure_runner_supervisor(%{runner_supervisor: nil} = state) do
    # The DynamicSupervisor is intentionally unnamed, so find the sibling by its
    # stable child id after both children have entered the pool supervisor.
    runner_supervisor =
      state.pool
      |> Supervisor.which_children()
      |> Enum.find_value(fn
        {id, pid, :supervisor, _modules} when id == state.runner_supervisor_id -> pid
        _child -> nil
      end)

    state
    |> Map.put(:runner_supervisor, runner_supervisor)
    |> monitor_existing_runners()
  end

  defp ensure_runner_supervisor(state), do: state

  defp monitor_existing_runners(%{runner_supervisor: nil} = state) do
    # Startup ordering should make this uncommon, but a short retry keeps a
    # transient supervision race from becoming a crash loop.
    schedule_poll(state, 10)
  end

  defp monitor_existing_runners(state) do
    # With :rest_for_one, a poller restart does not terminate older runners.
    # Rebuild the monitor set before claiming so their occupied capacity remains
    # accounted for and the restarted poller cannot over-claim.
    monitored = MapSet.new(Map.values(state.active))

    active =
      state.runner_supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.reduce(state.active, fn
        {_id, pid, :worker, _modules}, active when is_pid(pid) ->
          if MapSet.member?(monitored, pid) do
            active
          else
            Map.put(active, Process.monitor(pid), pid)
          end

        _child, active ->
          active
      end)

    %{state | active: active}
  end

  defp claim_available_capacity(%{runner_supervisor: nil} = state), do: state

  defp claim_available_capacity(state) do
    # `batch_size` bounds each database round trip; local capacity is the harder
    # limit and may be smaller while other runners are still active.
    capacity = max(state.concurrency - map_size(state.active), 0)

    if capacity == 0 do
      state
    else
      claim_tasks(state, min(capacity, state.batch_size))
    end
  end

  defp claim_tasks(state, amount) do
    options = [
      batch_size: amount,
      claim_timeout: state.claim_timeout,
      query_options: state.query_options
    ]

    case SQL.claim_tasks(state.db, state.queue, state.worker_id, options) do
      {:ok, tasks} ->
        # A successful database round trip proves the dependency recovered, even
        # when it returned no rows, so the next failure starts a fresh backoff.
        dispatch_claimed_tasks(%{state | error_attempt: 0}, tasks, amount)

      {:error, %Error{} = error} ->
        Logger.error(
          "Absurd worker poll failed for queue #{inspect(state.queue)} " <>
            "with #{inspect(error.kind)}"
        )

        schedule_error_backoff(state)
    end
  end

  defp dispatch_claimed_tasks(state, [], _amount) do
    schedule_poll(state, state.poll_interval)
  end

  defp dispatch_claimed_tasks(state, tasks, amount) do
    {state, failed_starts} =
      Enum.reduce(tasks, {state, 0}, fn task, {current, failed_starts} ->
        case start_runner(current, task) do
          {:ok, next} -> {next, failed_starts}
          {:error, next} -> {next, failed_starts + 1}
        end
      end)

    remaining_capacity = max(state.concurrency - map_size(state.active), 0)

    # A full claim batch is evidence that more work may already be available.
    # Fill the remaining local capacity without an artificial interval. An empty
    # or partial batch goes back to normal polling to avoid hammering PostgreSQL.
    cond do
      failed_starts > 0 -> schedule_poll(state, state.poll_interval)
      length(tasks) == amount and remaining_capacity > 0 -> schedule_poll(state, 0)
      true -> schedule_poll(state, state.poll_interval)
    end
  end

  defp start_runner(state, task) do
    options = [
      db: state.db,
      queue: state.queue,
      catalog: state.catalog,
      task: task,
      worker_id: state.worker_id,
      claim_timeout: state.claim_timeout,
      query_options: state.query_options,
      hooks: state.hooks
    ]

    case DynamicSupervisor.start_child(state.runner_supervisor, {Runner, options}) do
      {:ok, pid} ->
        reference = Process.monitor(pid)
        {:ok, %{state | active: Map.put(state.active, reference, pid)}}

      {:ok, pid, _info} ->
        reference = Process.monitor(pid)
        {:ok, %{state | active: Map.put(state.active, reference, pid)}}

      {:error, reason} ->
        Logger.error(
          "Absurd could not start runner #{inspect(task.task_name, printable_limit: 128)} on " <>
            "#{inspect(state.queue)}: #{inspect(reason, limit: 20, printable_limit: 1_000)}"
        )

        # The row is already claimed even though no runner exists. Explicitly
        # release it instead of making another worker wait for lease expiration.
        release_unstarted_claim(state, task)
        {:error, state}
    end
  end

  defp release_unstarted_claim(state, task) do
    case SQL.schedule_run_after(
           state.db,
           state.queue,
           task.run_id,
           state.poll_interval,
           state.query_options
         ) do
      :ok ->
        :ok

      {:error, %Error{} = error} ->
        Logger.error(
          "Absurd could not release unstarted task " <>
            "#{inspect(task.task_name, printable_limit: 128)} on " <>
            "#{inspect(state.queue)}: #{inspect(error.kind)}"
        )
    end
  end

  defp schedule_error_backoff(state) do
    # Database failures use capped exponential backoff with positive jitter. The
    # jitter spreads reconnect traffic across pools after a shared outage.
    base =
      min(
        state.poll_interval * Integer.pow(2, min(state.error_attempt, 16)),
        @maximum_error_backoff
      )

    jitter_window = div(base, 4)
    jitter = if jitter_window == 0, do: 0, else: :rand.uniform(jitter_window + 1) - 1
    delay = min(base + jitter, @maximum_error_backoff)

    state
    |> Map.update!(:error_attempt, &(&1 + 1))
    |> schedule_poll(delay)
  end

  defp schedule_poll(state, delay) do
    # Replace, rather than accumulate, future polls. The token also makes any
    # already-delivered message from the cancelled timer harmless.
    cancel_timer(state.poll_timer)
    token = make_ref()
    timer = Process.send_after(self(), {:poll, token}, delay)
    %{state | poll_timer: timer, poll_token: token}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer, async: true, info: false)

  defp drain_runners(active, _deadline) when map_size(active) == 0, do: :ok

  defp drain_runners(active, deadline) do
    # Every recursive wait shares one fixed deadline. A stream of runner exits
    # must not reset the full shutdown allowance after each DOWN message.
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:DOWN, reference, :process, _pid, _reason} ->
        drain_runners(Map.delete(active, reference), deadline)
    after
      remaining -> :ok
    end
  end
end
