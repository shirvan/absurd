defmodule Absurd.ContextPostgreSQLTest do
  use Absurd.PostgreSQLCase

  alias Absurd.Client
  alias Absurd.Context
  alias Absurd.Error
  alias Absurd.SpawnResult
  alias Absurd.SQL
  alias Absurd.Step
  alias Absurd.TaskResult

  test "replays null and repeated checkpoint occurrences across attempts", %{
    client: client,
    db: db,
    queue: queue
  } do
    assert {:ok, spawned} =
             Client.spawn(client, "checkpoint-task", %{"value" => 1},
               queue: queue,
               max_attempts: 2,
               retry_strategy: [kind: :fixed, base_seconds: 0]
             )

    assert {:ok, [first_claim]} = claim_one(db, queue)
    test_pid = self()

    assert {:ok, first_context} =
             Context.new(db, queue, first_claim,
               worker_id: "context-worker",
               claim_timeout: 1_500,
               lease_notifier: &send(test_pid, {:lease_extended, &1})
             )

    assert {:ok,
            %Step{
              name: "repeat",
              checkpoint_name: "repeat",
              done: false
            } = first_handle} = Context.begin_step(first_context, "repeat")

    expected_key = Base.encode16(spawned.task_id, case: :lower) <> ":repeat"
    assert Context.idempotency_key(first_context, first_handle) == expected_key
    assert {:ok, nil} = Context.complete_step(first_context, first_handle, nil)
    assert_received {:lease_extended, 2_000}

    assert {:ok, %{"occurrence" => 2}} =
             Context.step(first_context, "repeat", fn ->
               {:ok, %{"occurrence" => 2}}
             end)

    assert_received {:lease_extended, 2_000}

    assert {:ok, [nil, true, 3]} =
             Context.step(first_context, "shape", fn -> {:ok, [nil, true, 3]} end)

    assert_received {:lease_extended, 2_000}
    assert :ok = Context.close(first_context)
    assert :ok = Context.close(first_context)

    assert :ok = SQL.fail_run(db, queue, first_claim.run_id, %{"message" => "retry"})
    assert {:ok, [second_claim]} = claim_one(db, queue)
    assert second_claim.task_id == spawned.task_id
    assert second_claim.attempt == 2

    assert {:ok, second_context} = Context.new(db, queue, second_claim)

    assert {:ok, %Step{done: true, value: nil} = replayed_handle} =
             Context.begin_step(second_context, "repeat")

    assert {:ok, nil} = Context.complete_step(second_context, replayed_handle, "ignored")

    assert {:ok, %{"occurrence" => 2}} =
             Context.step(second_context, "repeat", fn ->
               flunk("a committed repeated checkpoint must not execute again")
             end)

    assert {:ok, [nil, true, 3]} =
             Context.step(second_context, "shape", fn ->
               flunk("a committed checkpoint must not execute again")
             end)

    assert :ok = SQL.complete_run(db, queue, second_claim.run_id, %{"replayed" => true})
    assert :ok = Context.close(second_context)

    assert {:ok, %TaskResult{state: :completed, result: %{"replayed" => true}}} =
             Client.fetch_task_result(client, spawned)
  end

  test "heartbeats round leases and durable sleep releases the run", %{
    client: client,
    db: db,
    queue: queue
  } do
    assert {:ok, spawned} = Client.spawn(client, "sleep-task", nil, queue: queue)
    assert {:ok, [claimed]} = claim_one(db, queue)
    test_pid = self()

    assert {:ok, context} =
             Context.new(db, queue, claimed,
               claim_timeout: 1_500,
               lease_notifier: &send(test_pid, {:lease_extended, &1})
             )

    assert :ok = Context.heartbeat(context)
    assert_received {:lease_extended, 2_000}

    assert :ok =
             Context.sleep_until(
               context,
               "already-awake",
               DateTime.add(DateTime.utc_now(), -1, :second)
             )

    assert_received {:lease_extended, 2_000}

    assert catch_throw(Context.sleep_for(context, "nap", 2_000)) ==
             {:absurd_context_control, context.control_ref, :suspended}

    assert_received {:lease_extended, 2_000}
    assert :ok = Context.close(context)
    assert {:ok, %TaskResult{state: :sleeping}} = Client.fetch_task_result(client, spawned)
  end

  test "event waits suspend, replay the first payload, and time out once", %{
    client: client,
    db: db,
    queue: queue
  } do
    base = ~U[2026-03-04 05:06:07Z]
    set_fake_now(db, base)
    on_exit(fn -> clear_fake_now(db) end)

    assert {:ok, event_task} = Client.spawn(client, "event-task", nil, queue: queue)
    assert {:ok, [first_claim]} = claim_one(db, queue)
    assert {:ok, first_context} = Context.new(db, queue, first_claim)
    event_name = "ready-#{queue}"

    assert catch_throw(Context.await_event(first_context, event_name)) ==
             {:absurd_context_control, first_context.control_ref, :suspended}

    assert :ok = Context.close(first_context)
    assert {:ok, %TaskResult{state: :sleeping}} = Client.fetch_task_result(client, event_task)

    payload = %{"sequence" => 1}
    assert :ok = Client.emit_event(client, event_name, payload)
    assert :ok = Client.emit_event(client, event_name, %{"sequence" => 2})
    assert {:ok, [second_claim]} = claim_one(db, queue)
    assert second_claim.event_payload == payload
    assert {:ok, second_context} = Context.new(db, queue, second_claim)
    assert {:ok, ^payload} = Context.await_event(second_context, event_name)
    assert :ok = SQL.complete_run(db, queue, second_claim.run_id, payload)
    assert :ok = Context.close(second_context)

    assert {:ok, timeout_task} = Client.spawn(client, "timeout-task", nil, queue: queue)
    assert {:ok, [timeout_claim]} = claim_one(db, queue)
    assert {:ok, timeout_context} = Context.new(db, queue, timeout_claim)
    timeout_event = "never-#{queue}"

    assert catch_throw(Context.await_event(timeout_context, timeout_event, timeout: 10_000)) ==
             {:absurd_context_control, timeout_context.control_ref, :suspended}

    assert :ok = Context.close(timeout_context)
    set_fake_now(db, DateTime.add(base, 10, :second))
    assert {:ok, [resumed_claim]} = claim_one(db, queue)
    assert resumed_claim.task_id == timeout_task.task_id
    assert resumed_claim.wake_event == timeout_event
    assert resumed_claim.event_payload == nil
    assert {:ok, resumed_context} = Context.new(db, queue, resumed_claim)

    assert {:error, %Error{kind: :timeout, operation: :await_event}} =
             Context.await_event(resumed_context, timeout_event, timeout: 10_000)

    assert :ok = SQL.fail_run(db, queue, resumed_claim.run_id, %{"message" => "timed out"})
    assert :ok = Context.close(resumed_context)
  end

  test "cross-queue child waits checkpoint terminal snapshots for replay", %{
    client: client,
    db: db,
    queue: queue
  } do
    child_queue = unique_queue("children")
    child_client = Client.new!(db: db, queue: child_queue)
    assert :ok = Client.create_queue(child_client)
    on_exit(fn -> assert :ok = Client.drop_queue(child_client) end)

    assert {:ok, child} =
             Client.spawn(child_client, "child", %{"child" => true}, queue: child_queue)

    assert {:ok, [child_claim]} = claim_one(db, child_queue)
    child_result = %{"answer" => 42}
    assert :ok = SQL.complete_run(db, child_queue, child_claim.run_id, child_result)

    assert {:ok, parent} =
             Client.spawn(client, "parent", nil,
               queue: queue,
               max_attempts: 2,
               retry_strategy: [kind: :fixed, base_seconds: 0]
             )

    assert {:ok, [first_parent_claim]} = claim_one(db, queue)
    assert {:ok, first_context} = Context.new(db, queue, first_parent_claim)

    assert {:error, %Error{kind: :validation}} =
             Context.await_task_result(first_context, %{child | task_id: <<1>>})

    assert {:error, %Error{kind: :validation}} =
             Context.await_task_result(first_context, %{child | queue: ""})

    assert {:ok, %TaskResult{state: :completed, result: ^child_result}} =
             Context.await_task_result(first_context, child)

    checkpoint_name = "$awaitTaskResult:" <> Base.encode16(child.task_id, case: :lower)

    assert {:ok, %{checkpoint_name: ^checkpoint_name}} =
             SQL.get_task_checkpoint_state(db, queue, parent.task_id, checkpoint_name)

    assert {:error, %Error{kind: :configuration}} =
             Context.await_task_result(first_context, parent)

    missing_child = %SpawnResult{
      queue: child_queue,
      task_id: :crypto.strong_rand_bytes(16),
      run_id: :crypto.strong_rand_bytes(16),
      attempt: 1,
      created: true
    }

    assert {:error, %Error{kind: :unknown_task}} =
             Context.await_task_result(first_context, missing_child)

    assert :ok = Context.close(first_context)

    assert :ok =
             SQL.fail_run(db, queue, first_parent_claim.run_id, %{"message" => "retry parent"})

    assert :ok = Client.drop_queue(child_client)
    assert {:ok, [second_parent_claim]} = claim_one(db, queue)
    assert {:ok, second_context} = Context.new(db, queue, second_parent_claim)

    assert {:ok, %TaskResult{state: :completed, result: ^child_result}} =
             Context.await_task_result(second_context, child)

    assert :ok = SQL.complete_run(db, queue, second_parent_claim.run_id, child_result)
    assert :ok = Context.close(second_context)
  end

  test "terminal database states become private runner controls", %{
    client: client,
    db: db,
    queue: queue
  } do
    assert {:ok, cancelled} = Client.spawn(client, "cancelled", nil, queue: queue)
    assert {:ok, [cancelled_claim]} = claim_one(db, queue)
    assert {:ok, cancelled_context} = Context.new(db, queue, cancelled_claim)
    assert :ok = Client.cancel_task(client, cancelled)

    assert catch_throw(Context.heartbeat(cancelled_context)) ==
             {:absurd_context_control, cancelled_context.control_ref, :cancelled}

    assert :ok = Context.close(cancelled_context)

    assert {:ok, failed} =
             Client.spawn(client, "failed", nil, queue: queue, max_attempts: 1)

    assert {:ok, [failed_claim]} = claim_one(db, queue)
    assert {:ok, failed_context} = Context.new(db, queue, failed_claim)
    assert :ok = SQL.fail_run(db, queue, failed_claim.run_id, %{"message" => "failed"})

    assert catch_throw(Context.heartbeat(failed_context)) ==
             {:absurd_context_control, failed_context.control_ref, :failed_run}

    assert :ok = Context.close(failed_context)
    assert {:ok, %TaskResult{state: :failed}} = Client.fetch_task_result(client, failed)
  end

  defp claim_one(db, queue) do
    SQL.claim_tasks(db, queue, "context-worker", claim_timeout: 60_000)
  end
end
