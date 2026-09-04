defmodule Absurd.PostgreSQLTest do
  use Absurd.PostgreSQLCase

  alias Absurd.Checkpoint
  alias Absurd.ClaimedTask
  alias Absurd.CleanupResult
  alias Absurd.Client
  alias Absurd.Error
  alias Absurd.EventWait
  alias Absurd.QueuePolicy
  alias Absurd.SpawnResult
  alias Absurd.SQL
  alias Absurd.TaskResult

  test "verifies the pinned schema and rejects a mismatch", %{db: db} do
    assert {:ok, "0.5.0"} = SQL.schema_version(db)

    assert {:error, :schema_mismatch_checked} =
             Postgrex.transaction(db, fn connection ->
               Postgrex.query!(
                 connection,
                 """
                 CREATE OR REPLACE FUNCTION absurd.get_schema_version()
                 RETURNS text
                 LANGUAGE sql
                 AS $$ SELECT 'integration-mismatch'::text $$
                 """,
                 []
               )

               assert {:error,
                       %Error{
                         kind: :schema_incompatible,
                         metadata: %{
                           expected: "0.5.0",
                           actual: "integration-mismatch"
                         }
                       }} = SQL.verify_schema_version(connection)

               Postgrex.rollback(connection, :schema_mismatch_checked)
             end)

    assert :ok = SQL.verify_schema_version(db)
  end

  test "manages queue storage and policy through clients and checked-out connections", %{
    client: client,
    db: db,
    queue: queue
  } do
    assert {:ok, %QueuePolicy{queue_name: ^queue, storage_mode: :unpartitioned}} =
             Client.get_queue_policy(client)

    assert :ok =
             Client.set_queue_policy(client,
               cleanup_ttl: "4321 seconds",
               cleanup_limit: 12
             )

    assert {:ok, %QueuePolicy{cleanup_ttl: cleanup_ttl, cleanup_limit: 12}} =
             Client.get_queue_policy(client)

    assert String.ends_with?(cleanup_ttl, "01:12:01")

    partitioned_queue = unique_queue("partitioned")
    on_exit(fn -> assert :ok = Client.drop_queue(client, partitioned_queue) end)

    assert :ok =
             Client.create_queue(client, partitioned_queue,
               storage_mode: :partitioned,
               partition_lookahead: "35 days",
               partition_lookback: "2 days",
               cleanup_ttl: "12345 seconds",
               cleanup_limit: 77,
               detach_mode: :empty,
               detach_min_age: "45 days"
             )

    assert {:ok,
            %QueuePolicy{
              queue_name: ^partitioned_queue,
              storage_mode: :partitioned,
              partition_lookahead: "35 days",
              partition_lookback: "2 days",
              cleanup_limit: 77,
              detach_mode: :empty,
              detach_min_age: "45 days"
            }} = Client.get_queue_policy(client, partitioned_queue)

    assert {:ok, queues} = Client.list_queues(client)
    assert queue in queues
    assert partitioned_queue in queues

    transaction_queue = unique_queue("transaction")
    on_exit(fn -> assert :ok = SQL.drop_queue(db, transaction_queue) end)

    assert {:error, :rollback_checked} =
             Postgrex.transaction(db, fn connection ->
               assert :ok = SQL.create_queue(connection, transaction_queue)

               assert {:ok, %QueuePolicy{queue_name: ^transaction_queue}} =
                        SQL.get_queue_policy(connection, transaction_queue)

               Postgrex.rollback(connection, :rollback_checked)
             end)

    assert {:ok, nil} = SQL.get_queue_policy(db, transaction_queue)
    assert :ok = Client.drop_queue(client, partitioned_queue)
    assert {:ok, nil} = Client.get_queue_policy(client, partitioned_queue)
  end

  test "spawns idempotently and completes a claimed run with checkpoints", %{
    client: client,
    db: db,
    queue: queue
  } do
    params = %{"message" => "hello", "nested" => [%{"ok" => true}]}
    idempotency_key = "idempotency-#{queue}"

    spawn_options = [
      queue: queue,
      max_attempts: 3,
      retry_strategy: [kind: :fixed, base_seconds: 0],
      headers: %{"trace-id" => "trace-1"},
      cancellation: [max_duration: 600, max_delay: 600],
      idempotency_key: idempotency_key
    ]

    assert {:ok,
            %SpawnResult{
              queue: ^queue,
              attempt: 1,
              created: true,
              task_id: task_id,
              run_id: run_id
            } = spawned} = Client.spawn(client, "echo", params, spawn_options)

    assert byte_size(task_id) == 16
    assert byte_size(run_id) == 16

    assert {:ok,
            %SpawnResult{
              task_id: ^task_id,
              run_id: ^run_id,
              attempt: 1,
              created: false
            }} = Client.spawn(client, "ignored", %{"message" => "ignored"}, spawn_options)

    assert {:ok, %TaskResult{state: :pending, result: nil, failure: nil}} =
             Client.fetch_task_result(client, spawned)

    assert {:ok,
            [
              %ClaimedTask{
                task_id: ^task_id,
                run_id: ^run_id,
                task_name: "echo",
                attempt: 1,
                params: ^params,
                retry_strategy: %{"base_seconds" => 0, "kind" => "fixed"},
                max_attempts: 3,
                headers: %{"trace-id" => "trace-1"}
              }
            ]} = SQL.claim_tasks(db, queue, "integration-worker", claim_timeout: 60_000)

    assert :ok = SQL.extend_claim(db, queue, run_id, 90_000)

    checkpoint_state = %{"answer" => 42}

    assert :ok =
             SQL.set_task_checkpoint_state(
               db,
               queue,
               task_id,
               "calculate",
               checkpoint_state,
               run_id,
               extend_claim_by: 90_000
             )

    assert {:ok,
            %Checkpoint{
              checkpoint_name: "calculate",
              state: ^checkpoint_state,
              status: "committed",
              owner_run_id: ^run_id,
              updated_at: %DateTime{}
            }} = SQL.get_task_checkpoint_state(db, queue, task_id, "calculate")

    assert {:ok, [%Checkpoint{checkpoint_name: "calculate", state: ^checkpoint_state}]} =
             SQL.get_task_checkpoint_states(db, queue, task_id, run_id)

    result = %{"status" => "ok"}
    assert :ok = SQL.complete_run(db, queue, run_id, result)

    assert {:ok, %TaskResult{state: :completed, result: ^result, failure: nil}} =
             Client.fetch_task_result(client, spawned)

    assert {:ok, %TaskResult{state: :completed, result: ^result}} =
             Client.await_task_result(client, spawned, timeout: 100)

    assert {:ok, []} = SQL.claim_tasks(db, queue, "integration-worker")
  end

  test "schedules runs with absolute and relative database time", %{
    client: client,
    db: db,
    queue: queue
  } do
    base = ~U[2026-01-02 03:04:05Z]
    set_fake_now(db, base)
    on_exit(fn -> clear_fake_now(db) end)

    assert {:ok, first} = spawn_raw(client, queue, "absolute-schedule")
    assert {:ok, [%ClaimedTask{run_id: first_run_id}]} = claim_one(db, queue)

    wake_at = DateTime.add(base, 60, :second)
    assert :ok = SQL.schedule_run(db, queue, first_run_id, wake_at)
    assert {:ok, %TaskResult{state: :sleeping}} = Client.fetch_task_result(client, first)

    set_fake_now(db, wake_at)

    assert {:ok, [%ClaimedTask{run_id: ^first_run_id}]} = claim_one(db, queue)
    assert :ok = SQL.complete_run(db, queue, first_run_id, %{"mode" => "absolute"})

    assert {:ok, second} = spawn_raw(client, queue, "relative-schedule")
    assert {:ok, [%ClaimedTask{run_id: second_run_id}]} = claim_one(db, queue)
    assert :ok = SQL.schedule_run_after(db, queue, second_run_id, 0)
    assert {:ok, %TaskResult{state: :sleeping}} = Client.fetch_task_result(client, second)
    assert {:ok, [%ClaimedTask{run_id: ^second_run_id}]} = claim_one(db, queue)
    assert :ok = SQL.complete_run(db, queue, second_run_id, %{"mode" => "relative"})
  end

  test "fails, retries, and cancels runs with terminal SQLSTATE mapping", %{
    client: client,
    db: db,
    queue: queue
  } do
    assert {:ok, spawned} =
             Client.spawn(client, "retry-me", %{"value" => 1},
               queue: queue,
               max_attempts: 1
             )

    assert {:ok, [%ClaimedTask{run_id: first_run_id}]} = claim_one(db, queue)
    failure = %{"name" => "RuntimeError", "message" => "boom"}
    assert :ok = SQL.fail_run(db, queue, first_run_id, failure)

    assert {:ok, %TaskResult{state: :failed, result: nil, failure: ^failure}} =
             Client.fetch_task_result(client, spawned)

    assert {:error, %Error{kind: :failed_run, sqlstate: "AB002"}} =
             SQL.fail_run(db, queue, first_run_id, failure)

    assert {:ok,
            %SpawnResult{
              task_id: retried_task_id,
              run_id: retry_run_id,
              attempt: 2,
              created: false
            } = retried} = Client.retry_task(client, spawned, max_attempts: 2)

    assert retried_task_id == spawned.task_id
    assert {:ok, [%ClaimedTask{run_id: ^retry_run_id}]} = claim_one(db, queue)
    assert :ok = SQL.fail_run(db, queue, retry_run_id, %{"message" => "again"})

    assert {:ok, %SpawnResult{attempt: 1, created: true} = replacement} =
             Client.retry_task(client, retried, spawn_new: true)

    assert replacement.task_id != spawned.task_id
    assert {:ok, [%ClaimedTask{run_id: replacement_run_id}]} = claim_one(db, queue)
    assert :ok = Client.cancel_task(client, replacement)

    assert {:ok, %TaskResult{state: :cancelled, result: nil, failure: nil}} =
             Client.fetch_task_result(client, replacement)

    assert {:error, %Error{kind: :cancelled, sqlstate: "AB001"}} =
             SQL.extend_claim(db, queue, replacement_run_id, 30_000)
  end

  test "suspends for events, preserves first-write-wins, and cleans retained data", %{
    client: client,
    db: db,
    queue: queue
  } do
    base = ~U[2026-02-03 04:05:06Z]
    set_fake_now(db, base)
    on_exit(fn -> clear_fake_now(db) end)

    assert {:ok, spawned} = spawn_raw(client, queue, "event-waiter")

    assert {:ok, [%ClaimedTask{task_id: task_id, run_id: run_id}]} = claim_one(db, queue)

    event_name = "event-#{queue}"

    assert {:ok, %EventWait{should_suspend: true, payload: nil}} =
             SQL.await_event(
               db,
               queue,
               task_id,
               run_id,
               "await-result",
               event_name,
               timeout: 60_000
             )

    assert {:ok, %TaskResult{state: :sleeping}} = Client.fetch_task_result(client, spawned)

    first_payload = %{"value" => 1}
    assert :ok = Client.emit_event(client, event_name, first_payload)
    assert :ok = Client.emit_event(client, event_name, %{"value" => 2})

    assert {:ok,
            [
              %ClaimedTask{
                task_id: ^task_id,
                run_id: ^run_id,
                wake_event: nil,
                event_payload: ^first_payload
              }
            ]} = claim_one(db, queue)

    assert {:ok, %EventWait{should_suspend: false, payload: ^first_payload}} =
             SQL.await_event(db, queue, task_id, run_id, "await-result", event_name)

    assert :ok = SQL.complete_run(db, queue, run_id, first_payload)

    set_fake_now(db, DateTime.add(base, 7_200, :second))

    assert {:ok, 1} = SQL.cleanup_tasks(db, queue, ttl: 3_600_000, limit: 10)
    assert {:ok, 1} = SQL.cleanup_events(db, queue, ttl: 3_600_000, limit: 10)
    assert {:ok, nil} = Client.fetch_task_result(client, spawned)

    assert :ok = Client.set_queue_policy(client, cleanup_ttl: "1 hour", cleanup_limit: 10)
    assert {:ok, another} = spawn_raw(client, queue, "policy-cleanup")
    assert {:ok, [%ClaimedTask{run_id: another_run_id}]} = claim_one(db, queue)
    assert :ok = SQL.complete_run(db, queue, another_run_id, %{"done" => true})
    assert :ok = Client.emit_event(client, "cleanup-#{queue}", %{"done" => true})

    set_fake_now(db, DateTime.add(base, 14_400, :second))

    assert {:ok,
            [
              %CleanupResult{
                queue_name: ^queue,
                tasks_deleted: 1,
                events_deleted: 1
              }
            ]} = SQL.cleanup_all_queues(db, queue)

    assert {:ok, nil} = Client.fetch_task_result(client, another)
  end

  defp spawn_raw(client, queue, task_name) do
    Client.spawn(client, task_name, %{"task" => task_name}, queue: queue)
  end

  defp claim_one(db, queue) do
    SQL.claim_tasks(db, queue, "integration-worker", claim_timeout: 60_000)
  end
end
