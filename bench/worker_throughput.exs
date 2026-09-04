# A dependency-free end-to-end throughput check for the worker path.
#
# This is a comparison tool, not a pass/fail benchmark. It includes sequential
# task creation, worker execution, checkpoint preload, finalization, and result
# observation through one Postgrex connection. Compare runs on the same database
# and machine rather than treating the number as a universal capacity claim.
#
# Postgrex reads the standard PGHOST, PGPORT, PGDATABASE, PGUSER, and PGPASSWORD
# environment variables. Optional benchmark settings are:
#
#   ABSURD_BENCH_TASKS=500
#   ABSURD_BENCH_CONCURRENCY=10
#   ABSURD_BENCH_BATCH_SIZE=10
#   ABSURD_BENCH_POLL_INTERVAL_MS=25
#   ABSURD_BENCH_TIMEOUT_MS=120000
#
# Run with:
#
#   PGDATABASE=absurd_test PGUSER=postgres PGPASSWORD=postgres \
#     mix run bench/worker_throughput.exs

defmodule Absurd.Bench.NoopTask do
  @moduledoc false

  use Absurd.Task, name: "benchmark-noop"

  @impl Absurd.Task
  def run(params, _context), do: {:ok, params}
end

defmodule Absurd.Bench.WorkerThroughput do
  @moduledoc false

  alias Absurd.Bench.NoopTask
  alias Absurd.Client
  alias Absurd.TaskResult
  alias Absurd.WorkerPool

  @spec run() :: :ok
  def run do
    settings = settings()
    {:ok, db} = Postgrex.start_link([])

    queue = unique_queue()
    client = Absurd.client(db: db, queue: queue)

    try do
      :ok = Absurd.SQL.verify_schema_version(db)
      :ok = Client.create_queue(client)
      run_measurement(db, client, queue, settings)
    after
      Client.drop_queue(client)
      stop_process(db)
    end
  end

  defp run_measurement(db, client, queue, settings) do
    worker_options = [
      name: {:global, {__MODULE__, make_ref()}},
      db: db,
      queue: queue,
      tasks: [NoopTask],
      concurrency: settings.concurrency,
      batch_size: settings.batch_size,
      poll_interval: settings.poll_interval
    ]

    {:ok, worker} = WorkerPool.start_link(worker_options)

    try do
      {elapsed_microseconds, completed} =
        :timer.tc(fn -> execute_tasks(client, settings.tasks, settings.timeout) end)

      seconds = elapsed_microseconds / 1_000_000
      tasks_per_second = completed / seconds

      IO.puts("""
      Absurd worker throughput
        tasks: #{completed}
        concurrency: #{settings.concurrency}
        batch size: #{settings.batch_size}
        poll interval: #{settings.poll_interval} ms
        elapsed: #{Float.round(seconds, 3)} s
        throughput: #{Float.round(tasks_per_second, 1)} tasks/s
      """)
    after
      stop_process(worker)
    end
  end

  defp execute_tasks(client, task_count, timeout) do
    tasks =
      Enum.map(1..task_count, fn sequence ->
        {:ok, spawned} = Client.spawn(client, NoopTask, %{"sequence" => sequence})
        spawned
      end)

    Enum.each(tasks, fn task ->
      case Client.await_task_result(client, task, timeout: timeout) do
        {:ok, %TaskResult{state: :completed}} -> :ok
        other -> raise "benchmark task did not complete: #{inspect(other)}"
      end
    end)

    length(tasks)
  end

  defp settings do
    concurrency = positive_setting("ABSURD_BENCH_CONCURRENCY", 10)

    %{
      tasks: positive_setting("ABSURD_BENCH_TASKS", 500),
      concurrency: concurrency,
      batch_size: positive_setting("ABSURD_BENCH_BATCH_SIZE", concurrency),
      poll_interval: positive_setting("ABSURD_BENCH_POLL_INTERVAL_MS", 25),
      timeout: positive_setting("ABSURD_BENCH_TIMEOUT_MS", 120_000)
    }
  end

  defp positive_setting(name, default) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {integer, ""} when integer > 0 -> integer
          _other -> raise ArgumentError, "#{name} must be a positive integer"
        end
    end
  end

  defp unique_queue do
    timestamp = System.system_time(:microsecond) |> Integer.to_string(36)
    suffix = System.unique_integer([:positive, :monotonic]) |> Integer.to_string(36)
    "elixir_bench_#{timestamp}_#{suffix}"
  end

  defp stop_process(process) do
    if Process.alive?(process) do
      GenServer.stop(process, :normal, 30_000)
    end
  catch
    :exit, _reason -> :ok
  end
end

Absurd.Bench.WorkerThroughput.run()
