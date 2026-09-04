defmodule Absurd.TestWorkerProbe do
  @moduledoc false

  @name Absurd.WorkerPostgreSQLTest.Probe

  @spec notify(term()) :: :ok
  def notify(message) do
    case Process.whereis(@name) do
      nil -> :ok
      pid -> send(pid, message)
    end

    :ok
  end
end

defmodule Absurd.TestWorkerTasks.Echo do
  @moduledoc false

  use Absurd.Task, name: "worker-echo"

  @impl Absurd.Task
  def run(params, context) do
    Absurd.TestWorkerProbe.notify({:echo_started, self(), params})
    {:ok, %{"echo" => params, "headers" => context.headers}}
  end
end

defmodule Absurd.TestWorkerTasks.Gate do
  @moduledoc false

  use Absurd.Task, name: "worker-gate"

  @impl Absurd.Task
  def run(%{"token" => token}, context) do
    Absurd.TestWorkerProbe.notify({:gate_started, token, self(), context.queue, context.task_id})

    receive do
      {:release, ^token} -> {:ok, %{"token" => token}}
    end
  end
end

defmodule Absurd.TestWorkerTasks.Replay do
  @moduledoc false

  use Absurd.Task, name: "worker-replay"

  @impl Absurd.Task
  def run(_params, context) do
    with {:ok, value} <-
           Absurd.Context.step(context, "effect", fn ->
             Absurd.TestWorkerProbe.notify({:checkpoint_effect, context.attempt})
             {:ok, %{"created_on_attempt" => context.attempt}}
           end) do
      Absurd.TestWorkerProbe.notify({:replay_attempt, context.attempt, value})

      if context.attempt == 1 do
        {:error, :retry_once}
      else
        {:ok, value}
      end
    end
  end
end

defmodule Absurd.TestWorkerTasks.Event do
  @moduledoc false

  use Absurd.Task, name: "worker-event"

  @impl Absurd.Task
  def run(%{"event" => event_name}, context) do
    Absurd.TestWorkerProbe.notify({:event_attempt, context.attempt})

    with {:ok, payload} <- Absurd.Context.await_event(context, event_name) do
      Absurd.TestWorkerProbe.notify({:event_received, payload})
      {:ok, payload}
    end
  end
end

defmodule Absurd.TestWorkerTasks.Raises do
  @moduledoc false

  use Absurd.Task, name: "worker-raises"

  @impl Absurd.Task
  def run(_params, _context) do
    raise String.duplicate("failure ", 1_000)
  end
end

defmodule Absurd.TestWorkerTasks.Hangs do
  @moduledoc false

  use Absurd.Task, name: "worker-hangs"

  @impl Absurd.Task
  def run(_params, _context) do
    Absurd.TestWorkerProbe.notify({:hang_started, self()})
    Process.sleep(:infinity)
  end
end

defmodule Absurd.TestWorkerHooks do
  @moduledoc false

  @behaviour Absurd.Hooks

  @impl Absurd.Hooks
  def wrap_task_execution(context, execute) do
    Absurd.TestWorkerProbe.notify({:hook_before, context.task_name})
    result = execute.()
    Absurd.TestWorkerProbe.notify({:hook_after, context.task_name})
    result
  end
end
