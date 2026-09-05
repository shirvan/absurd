defmodule Absurd.WorkerPostgreSQLTest do
  use Absurd.PostgreSQLCase

  import ExUnit.CaptureLog

  alias Absurd.Client
  alias Absurd.SQL
  alias Absurd.TaskResult
  alias Absurd.TestPostgreSQLResponseProxy
  alias Absurd.TestWorkerHooks
  alias Absurd.TestWorkerTasks.AmbiguousResult
  alias Absurd.TestWorkerTasks.Echo
  alias Absurd.TestWorkerTasks.Event
  alias Absurd.TestWorkerTasks.Gate
  alias Absurd.TestWorkerTasks.Hangs
  alias Absurd.TestWorkerTasks.Raises
  alias Absurd.TestWorkerTasks.Replay
  alias Absurd.TestWorkerTasks.UncertainWrite
  alias Absurd.WorkerPool

  @probe __MODULE__.Probe

  setup do
    Process.register(self(), @probe)
    :ok
  end

  test "runs registered tasks through hooks and finalizes JSON results", context do
    pool = start_pool(context, [Echo], hooks: TestWorkerHooks)

    telemetry_events = [
      [:absurd, :runner, :execute, :start],
      [:absurd, :runner, :execute, :stop]
    ]

    telemetry_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach_many(
        telemetry_id,
        telemetry_events,
        &Absurd.TestTelemetryHandler.handle_event/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    assert %{active: 2, workers: 1, supervisors: 1} = Supervisor.count_children(pool)

    params = %{"message" => "hello"}

    assert {:ok, spawned} =
             Client.spawn(context.client, Echo, params, headers: %{"trace_id" => "trace-1"})

    assert_receive {:hook_before, "worker-echo"}, 1_000
    assert_receive {:echo_started, runner, ^params}, 1_000
    assert is_pid(runner)
    assert_receive {:hook_after, "worker-echo"}, 1_000

    assert {:ok,
            %TaskResult{
              state: :completed,
              result: %{
                "echo" => ^params,
                "headers" => %{"trace_id" => "trace-1"}
              }
            }} = Client.await_task_result(context.client, spawned, timeout: 2_000)

    assert_receive {[:absurd, :runner, :execute, :start], %{system_time: _time}, start_metadata}

    assert start_metadata.task_name == "worker-echo"
    assert start_metadata.task_id == Base.encode16(spawned.task_id, case: :lower)

    assert_receive {[:absurd, :runner, :execute, :stop], %{duration: duration}, stop_metadata}

    assert is_integer(duration)
    assert stop_metadata.outcome == :completed
  end

  for {name, operation, expected_state} <- [
        {"sleep", :schedule_run_after, :sleeping},
        {"event", :await_event, :sleeping},
        {"checkpoint", :set_task_checkpoint_state, :running},
        {"heartbeat", :extend_claim, :running}
      ] do
    @tag :capture_log
    test "a lost #{name} response unwinds without a competing finalization", context do
      operation = unquote(operation)
      proxy = start_supervised!({TestPostgreSQLResponseProxy, context.db_options})

      db_options =
        Keyword.merge(context.db_options,
          hostname: "127.0.0.1",
          port: TestPostgreSQLResponseProxy.port(proxy),
          ssl: false
        )

      db = start_supervised!({Postgrex, db_options})
      _pool = start_pool(context, [UncertainWrite], db: db, claim_timeout: 10_000)
      attach_write_fault(proxy, operation)
      attach_finalization_events()

      assert {:ok, spawned} =
               Client.spawn(context.client, UncertainWrite, %{"operation" => unquote(name)},
                 max_attempts: 1
               )

      assert_receive {:response_dropped, ^operation}, 3_000

      assert_receive {[:absurd, :runner, :execute, :stop], _measurements, %{outcome: :ambiguous}},
                     3_000

      refute_received {[:absurd, :sql, :fail_run, :start], _, _}
      refute_received {[:absurd, :sql, :complete_run, :start], _, _}
      refute_received {:continued_after_write, _operation}

      assert {:ok, %TaskResult{state: unquote(expected_state)}} =
               Client.fetch_task_result(context.client, spawned)

      assert_committed_effect(operation, context, spawned)
    end
  end

  for raise? <- [false, true] do
    @tag :capture_log
    test "an ambiguous error #{if raise?, do: "raised", else: "returned"} by task code is preserved",
         context do
      _pool = start_pool(context, [AmbiguousResult])
      attach_finalization_events()

      assert {:ok, spawned} =
               Client.spawn(context.client, AmbiguousResult, %{"raise" => unquote(raise?)},
                 max_attempts: 1
               )

      assert_receive {[:absurd, :runner, :execute, :stop], _measurements, %{outcome: :ambiguous}},
                     2_000

      refute_received {[:absurd, :sql, :fail_run, :start], _, _}

      assert {:ok, %TaskResult{state: :sleeping}} =
               Client.fetch_task_result(context.client, spawned)
    end
  end

  test "retries from the beginning and replays committed checkpoints", context do
    _pool = start_pool(context, [Replay])

    assert {:ok, spawned} =
             Client.spawn(context.client, Replay, nil,
               max_attempts: 2,
               retry_strategy: [kind: :fixed, base_seconds: 0]
             )

    assert_receive {:checkpoint_effect, 1}, 1_000
    assert_receive {:replay_attempt, 1, %{"created_on_attempt" => 1}}, 1_000
    assert_receive {:replay_attempt, 2, %{"created_on_attempt" => 1}}, 2_000
    refute_receive {:checkpoint_effect, 2}, 100

    assert {:ok,
            %TaskResult{
              state: :completed,
              result: %{"created_on_attempt" => 1}
            }} = Client.await_task_result(context.client, spawned, timeout: 2_000)
  end

  test "event suspension releases capacity and resumes with the first payload", context do
    _pool = start_pool(context, [Echo, Event], concurrency: 1, batch_size: 1)
    event_name = "worker-ready-#{context.queue}"

    assert {:ok, event_task} =
             Client.spawn(context.client, Event, %{"event" => event_name})

    assert_receive {:event_attempt, 1}, 1_000

    assert {:ok, echo_task} = Client.spawn(context.client, Echo, %{"after" => "suspend"})

    assert {:ok, %TaskResult{state: :completed}} =
             Client.await_task_result(context.client, echo_task, timeout: 2_000)

    payload = %{"approved" => true}
    assert :ok = Client.emit_event(context.client, event_name, payload)
    assert_receive {:event_attempt, 1}, 2_000
    assert_receive {:event_received, ^payload}, 1_000

    assert {:ok, %TaskResult{state: :completed, result: ^payload}} =
             Client.await_task_result(context.client, event_task, timeout: 2_000)
  end

  test "never claims above capacity and refills immediately on runner exit", context do
    _pool = start_pool(context, [Gate], concurrency: 2, batch_size: 2)

    tasks =
      for token <- ["one", "two", "three"], into: %{} do
        assert {:ok, spawned} = Client.spawn(context.client, Gate, %{"token" => token})
        {token, spawned}
      end

    started =
      for _index <- 1..2, into: %{} do
        assert_receive {:gate_started, token, runner, _queue, _task_id}, 1_000
        {token, runner}
      end

    assert map_size(started) == 2
    refute_receive {:gate_started, _token, _runner, _queue, _task_id}, 100

    [{released_token, released_runner} | _rest] = Map.to_list(started)
    send(released_runner, {:release, released_token})

    remaining_token = Enum.find(Map.keys(tasks), &(not Map.has_key?(started, &1)))
    assert_receive {:gate_started, ^remaining_token, replacement_runner, _queue, _task_id}, 1_000

    for {token, runner} <- started, token != released_token do
      send(runner, {:release, token})
    end

    send(replacement_runner, {:release, remaining_token})

    for spawned <- Map.values(tasks) do
      assert {:ok, %TaskResult{state: :completed}} =
               Client.await_task_result(context.client, spawned, timeout: 2_000)
    end
  end

  test "a restarted poller counts surviving runners before claiming", context do
    pool = start_pool(context, [Gate], concurrency: 1, batch_size: 1)

    assert {:ok, first} = Client.spawn(context.client, Gate, %{"token" => "survivor"})
    assert {:ok, second} = Client.spawn(context.client, Gate, %{"token" => "waiting"})
    assert_receive {:gate_started, "survivor", runner, _queue, _task_id}, 1_000

    old_poller = poller_pid(pool)
    old_poller_ref = Process.monitor(old_poller)
    Process.exit(old_poller, :kill)
    assert_receive {:DOWN, ^old_poller_ref, :process, ^old_poller, :killed}, 1_000

    assert :ok = wait_until(fn -> poller_pid(pool) not in [nil, old_poller] end)
    refute_receive {:gate_started, "waiting", _runner, _queue, _task_id}, 100

    send(runner, {:release, "survivor"})
    assert_receive {:gate_started, "waiting", second_runner, _queue, _task_id}, 1_000
    send(second_runner, {:release, "waiting"})

    for spawned <- [first, second] do
      assert {:ok, %TaskResult{state: :completed}} =
               Client.await_task_result(context.client, spawned, timeout: 2_000)
    end
  end

  test "a runner supervisor failure restarts capacity accounting and the pool recovers",
       context do
    pool =
      start_pool(context, [Echo, Gate],
        concurrency: 1,
        batch_size: 1,
        shutdown: 100
      )

    assert {:ok, interrupted} =
             Client.spawn(context.client, Gate, %{"token" => "supervisor-failure"})

    assert_receive {:gate_started, "supervisor-failure", runner, _queue, _task_id}, 1_000

    old_runner_supervisor = runner_supervisor_pid(pool)
    old_poller = poller_pid(pool)
    runner_ref = Process.monitor(runner)
    runner_supervisor_ref = Process.monitor(old_runner_supervisor)
    poller_ref = Process.monitor(old_poller)

    Process.exit(old_runner_supervisor, :kill)

    assert_receive {:DOWN, ^runner_supervisor_ref, :process, ^old_runner_supervisor, :killed},
                   1_000

    assert_receive {:DOWN, ^runner_ref, :process, ^runner, _reason}, 1_000
    assert_receive {:DOWN, ^poller_ref, :process, ^old_poller, _reason}, 2_000

    assert :ok =
             wait_until(fn ->
               runner_supervisor_pid(pool) not in [nil, old_runner_supervisor] and
                 poller_pid(pool) not in [nil, old_poller]
             end)

    assert :ok = Client.cancel_task(context.client, interrupted)
    assert {:ok, next_task} = Client.spawn(context.client, Echo, %{"pool" => "recovered"})

    assert {:ok, %TaskResult{state: :completed}} =
             Client.await_task_result(context.client, next_task, timeout: 2_000)
  end

  test "cancellation wins a completion race without stopping the pool", context do
    _pool = start_pool(context, [Echo, Gate], concurrency: 1, batch_size: 1)

    assert {:ok, cancelled} = Client.spawn(context.client, Gate, %{"token" => "cancel"})
    assert_receive {:gate_started, "cancel", runner, _queue, _task_id}, 1_000
    assert :ok = Client.cancel_task(context.client, cancelled)
    send(runner, {:release, "cancel"})

    assert {:ok, %TaskResult{state: :cancelled}} =
             Client.await_task_result(context.client, cancelled, timeout: 2_000)

    assert {:ok, next_task} = Client.spawn(context.client, Echo, %{"pool" => "alive"})

    assert {:ok, %TaskResult{state: :completed}} =
             Client.await_task_result(context.client, next_task, timeout: 2_000)
  end

  test "defers unknown names for 15 to 30 seconds without consuming an attempt", context do
    base = ~U[2026-04-05 06:07:08Z]
    set_fake_now(context.db, base)
    on_exit(fn -> clear_fake_now(context.db) end)

    assert {:ok, spawned} =
             Client.spawn(context.client, "unknown-worker-task", nil, queue: context.queue)

    pool = start_pool(context, [])
    assert %TaskResult{state: :sleeping} = wait_for_state(context.client, spawned, :sleeping)
    assert :ok = Supervisor.stop(pool, :normal, 2_000)

    set_fake_now(context.db, DateTime.add(base, 14, :second))
    assert {:ok, []} = SQL.claim_tasks(context.db, context.queue, "manual")

    set_fake_now(context.db, DateTime.add(base, 31, :second))
    assert {:ok, [claimed]} = SQL.claim_tasks(context.db, context.queue, "manual")
    assert claimed.task_id == spawned.task_id
    assert claimed.attempt == 1
    assert :ok = SQL.complete_run(context.db, context.queue, claimed.run_id, nil)
  end

  test "serializes raised failures into bounded durable JSON", context do
    _pool = start_pool(context, [Raises])
    assert {:ok, spawned} = Client.spawn(context.client, Raises, nil, max_attempts: 1)

    assert {:ok,
            %TaskResult{
              state: :failed,
              failure: %{
                "name" => "RuntimeError",
                "message" => message,
                "stacktrace" => stacktrace
              }
            }} = Client.await_task_result(context.client, spawned, timeout: 2_000)

    assert byte_size(message) <= 4_096
    assert byte_size(stacktrace) <= 8_192
  end

  test "a hard lease timeout kills only the stuck runner and restores capacity", context do
    _pool =
      start_pool(context, [Echo, Hangs],
        concurrency: 1,
        batch_size: 1,
        claim_timeout: 1_000
      )

    assert {:ok, hung} = Client.spawn(context.client, Hangs, nil, max_attempts: 1)
    assert_receive {:hang_started, hung_runner}, 1_000
    hung_ref = Process.monitor(hung_runner)
    assert {:ok, echo} = Client.spawn(context.client, Echo, %{"after" => "watchdog"})

    log =
      capture_log(fn ->
        assert_receive {:DOWN, ^hung_ref, :process, ^hung_runner, :killed}, 3_000

        assert {:ok, %TaskResult{state: :failed}} =
                 Client.await_task_result(context.client, hung, timeout: 2_000)

        assert {:ok, %TaskResult{state: :completed}} =
                 Client.await_task_result(context.client, echo, timeout: 2_000)
      end)

    assert log =~ "exceeded its active claim lease"
    assert log =~ "terminating runner"
    assert Process.alive?(self())
  end

  test "shutdown stops new claims while allowing active runners to drain", context do
    pool =
      start_pool(context, [Gate],
        concurrency: 1,
        batch_size: 1,
        shutdown: 500
      )

    assert {:ok, first} = Client.spawn(context.client, Gate, %{"token" => "drain"})
    assert_receive {:gate_started, "drain", runner, queue, first_task_id}, 1_000
    assert queue == context.queue
    assert first_task_id == first.task_id

    children = Supervisor.which_children(pool)

    {_poller_id, poller, :worker, _modules} =
      Enum.find(children, &match?({Absurd.Poller, _, _, _}, &1))

    {_runner_id, runner_supervisor, :supervisor, _modules} =
      Enum.find(children, &match?({{WorkerPool, :runner_supervisor}, _, _, _}, &1))

    assert %{active: 1} = DynamicSupervisor.count_children(runner_supervisor)
    assert %{active: active} = :sys.get_state(poller)
    assert map_size(active) == 1

    stopper = Task.async(fn -> Supervisor.stop(pool, :normal, 2_000) end)

    assert :ok =
             wait_until(fn ->
               Process.info(poller, :current_function) ==
                 {:current_function, {Absurd.Poller, :drain_runners, 2}}
             end)

    assert {:ok, second} = Client.spawn(context.client, Gate, %{"token" => "pending"})
    send(runner, {:release, "drain"})
    assert :ok = Task.await(stopper, 2_000)

    assert {:ok, %TaskResult{state: :completed}} =
             Client.fetch_task_result(context.client, first)

    assert {:ok, %TaskResult{state: :pending}} =
             Client.fetch_task_result(context.client, second)

    refute_receive {:gate_started, "pending", _runner, _queue, _task_id}, 100
  end

  defp start_pool(context, tasks, overrides \\ []) do
    name = {:global, {__MODULE__, context.queue, System.unique_integer([:positive])}}

    options =
      [
        name: name,
        db: context.db,
        queue: context.queue,
        tasks: tasks,
        poll_interval: 20,
        claim_timeout: 5_000,
        shutdown: 1_000
      ]
      |> Keyword.merge(overrides)

    start_supervised!(Supervisor.child_spec({WorkerPool, options}, restart: :temporary))
  end

  defp attach_write_fault(proxy, operation) do
    id = {__MODULE__, :fault, make_ref()}

    :ok =
      :telemetry.attach(
        id,
        [:absurd, :sql, operation, :start],
        &TestPostgreSQLResponseProxy.arm_on_query/4,
        {proxy, self()}
      )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp attach_finalization_events do
    id = {__MODULE__, :finalization, make_ref()}

    events = [
      [:absurd, :runner, :execute, :stop],
      [:absurd, :sql, :fail_run, :start],
      [:absurd, :sql, :complete_run, :start]
    ]

    :ok = :telemetry.attach_many(id, events, &Absurd.TestTelemetryHandler.handle_event/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp assert_committed_effect(:set_task_checkpoint_state, context, spawned) do
    assert {:ok, %{state: 42}} =
             SQL.get_task_checkpoint_state(context.db, context.queue, spawned.task_id, "effect")
  end

  defp assert_committed_effect(_operation, _context, _spawned), do: :ok

  defp wait_for_state(client, task, expected_state) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    do_wait_for_state(client, task, expected_state, deadline)
  end

  defp do_wait_for_state(client, task, expected_state, deadline) do
    case Client.fetch_task_result(client, task) do
      {:ok, %TaskResult{state: ^expected_state} = result} ->
        result

      {:ok, %TaskResult{}} ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("task did not reach #{inspect(expected_state)} before the deadline")
        else
          Process.sleep(10)
          do_wait_for_state(client, task, expected_state, deadline)
        end

      {:error, error} ->
        flunk("could not read task state: #{inspect(error)}")
    end
  end

  defp wait_until(assertion) do
    deadline = System.monotonic_time(:millisecond) + 500
    do_wait_until(assertion, deadline)
  end

  defp do_wait_until(assertion, deadline) do
    cond do
      assertion.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition did not become true before the deadline")

      true ->
        Process.sleep(5)
        do_wait_until(assertion, deadline)
    end
  end

  defp poller_pid(pool) do
    pool
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {Absurd.Poller, pid, :worker, _modules} -> pid
      _child -> nil
    end)
  catch
    :exit, _reason -> nil
  end

  defp runner_supervisor_pid(pool) do
    pool
    |> Supervisor.which_children()
    |> Enum.find_value(fn
      {{WorkerPool, :runner_supervisor}, pid, :supervisor, _modules} -> pid
      _child -> nil
    end)
  catch
    :exit, _reason -> nil
  end
end
