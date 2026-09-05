defmodule Absurd.ContextRegressionPostgreSQLTest do
  use Absurd.PostgreSQLCase

  alias Absurd.Context
  alias Absurd.Error
  alias Absurd.SQL
  alias Absurd.Step
  alias Absurd.TaskResult

  test "rejects colliding occurrences without changing checkpoint naming", env do
    {_spawned, claimed} = spawn_claim(env)
    {:ok, context} = Context.new(env.db, env.queue, claimed)

    assert {:ok, 1} = Context.step(context, "charge", fn -> {:ok, 1} end)
    assert {:ok, 2} = Context.step(context, "charge", fn -> {:ok, 2} end)

    assert {:error, %Error{kind: :validation}} =
             Context.step(context, "charge#2", fn -> flunk("colliding effect executed") end)

    assert {:ok, %Step{checkpoint_name: "charge#3", done: false}} =
             Context.begin_step(context, "charge")

    assert {:ok, %Step{checkpoint_name: "literal#2", done: false}} =
             Context.begin_step(context, "literal#2")

    assert {:ok, %Step{checkpoint_name: "reverse#2", done: false}} =
             Context.begin_step(context, "reverse#2")

    assert {:ok, %Step{checkpoint_name: "reverse", done: false}} =
             Context.begin_step(context, "reverse")

    assert {:error, %Error{kind: :validation}} = Context.begin_step(context, "reverse")
    assert {:ok, 42} = Context.step(context, "$absurd:application", fn -> {:ok, 42} end)
    assert :ok = Context.close(context)
  end

  test "fresh steps use one bounded read and write and cached replay issues no queries", env do
    {_spawned, claimed} = spawn_claim(env)
    context = observed_context(env, claimed)
    payload = String.duplicate("x", 1_024)

    for index <- 1..50 do
      assert {:ok, ^payload} = Context.step(context, "step_#{index}", fn -> {:ok, payload} end)

      assert [{_read, 0}, {_write, 1}] =
               assert_queries([:get_task_checkpoint_state, :set_task_checkpoint_state])
    end

    Context.close(context)
    replay = observed_context(env, claimed)

    for index <- 1..50 do
      assert {:ok, ^payload} =
               Context.step(replay, "step_#{index}", fn -> flunk("cached effect executed") end)
    end

    assert_queries([])
    Context.close(replay)
  end

  test "event resolution uses two calls and persists only the event checkpoint", env do
    {spawned, claimed} = spawn_claim(env)
    :ok = SQL.emit_event(env.db, env.queue, "ready", nil)
    context = observed_context(env, claimed)

    assert {:ok, nil} = Context.await_event(context, "ready")
    assert_queries([:get_task_checkpoint_state, :await_event])

    assert {:ok, [%{checkpoint_name: "$awaitEvent:ready", state: nil}]} =
             SQL.get_task_checkpoint_states(env.db, env.queue, spawned.task_id, claimed.run_id)

    Context.close(context)
    replay = observed_context(env, claimed)
    assert {:ok, nil} = Context.await_event(replay, "ready")
    assert_queries([])
    Context.close(replay)
  end

  test "a timeout claim is reported without writing SDK timeout checkpoints", env do
    {spawned, claimed} = spawn_claim(env)
    context = observed_context(env, claimed)

    assert catch_throw(Context.await_event(context, "missing", timeout: 0)) ==
             {:absurd_context_control, context.control_ref, :suspended}

    assert_queries([:get_task_checkpoint_state, :await_event])
    Context.close(context)

    {:ok, [resumed]} = SQL.claim_tasks(env.db, env.queue, "resumed")
    assert resumed.wake_event == "missing"
    context = observed_context(env, resumed)
    assert {:error, %Error{kind: :timeout}} = Context.await_event(context, "missing", timeout: 0)
    assert_queries([:get_task_checkpoint_state])

    assert {:ok, []} =
             SQL.get_task_checkpoint_states(env.db, env.queue, spawned.task_id, resumed.run_id)

    Context.close(context)
  end

  test "a terminal child needs only a checkpoint read, result read, and checkpoint write", env do
    queue = unique_queue("child")
    :ok = SQL.create_queue(env.db, queue)
    on_exit(fn -> assert :ok = SQL.drop_queue(env.db, queue) end)
    {:ok, child} = SQL.spawn_task(env.db, queue, "child", nil)
    {:ok, [child_claim]} = SQL.claim_tasks(env.db, queue, "child-worker")
    :ok = SQL.complete_run(env.db, queue, child_claim.run_id, 42)
    {_spawned, claimed} = spawn_claim(env)
    context = observed_context(env, claimed)

    assert {:ok, %TaskResult{state: :completed, result: 42}} =
             Context.await_task_result(context, child)

    assert_queries([:get_task_checkpoint_state, :get_task_result, :set_task_checkpoint_state])
    Context.close(context)
  end

  defp spawn_claim(env) do
    {:ok, spawned} = SQL.spawn_task(env.db, env.queue, "regression", nil)
    {:ok, [claimed]} = SQL.claim_tasks(env.db, env.queue, "worker")
    {spawned, claimed}
  end

  defp observed_context(env, claimed) do
    observer = self()

    log = fn entry ->
      case entry.result do
        {:ok, _query, %Postgrex.Result{num_rows: count}} ->
          send(observer, {:query, to_string(entry.query), count})

        _other ->
          :ok
      end
    end

    {:ok, context} = Context.new(env.db, env.queue, claimed, query_options: [log: log])
    assert_queries([:get_task_checkpoint_states])
    context
  end

  defp assert_queries(operations) do
    queries = drain_queries([])

    observed =
      Enum.map(queries, fn {statement, _count} ->
        [_, operation] = Regex.run(~r/absurd\.(\w+)\(/, statement)
        operation
      end)

    assert observed == Enum.map(operations, &Atom.to_string/1)
    queries
  end

  defp drain_queries(queries) do
    receive do
      {:query, statement, count} -> drain_queries([{statement, count} | queries])
    after
      0 -> Enum.reverse(queries)
    end
  end
end
