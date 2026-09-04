defmodule Absurd.LeaseWatchdog do
  # The watchdog is isolated from its runner so it can observe time passing even
  # while user code blocks that runner's mailbox. PostgreSQL still owns the lease;
  # this process is a local backstop for callbacks that fail to heartbeat.
  @moduledoc false

  use GenServer

  require Logger

  # These functions form the private process boundary used by Absurd.Runner.
  @doc false
  @spec start_link(pid(), pos_integer(), map()) :: GenServer.on_start()
  def start_link(owner, lease_duration, metadata) do
    GenServer.start_link(__MODULE__, {owner, lease_duration, metadata})
  end

  @doc false
  @spec reset(GenServer.server(), pos_integer()) :: :ok
  def reset(watchdog, lease_duration) do
    GenServer.cast(watchdog, {:reset, lease_duration})
  end

  @doc false
  @spec stop(GenServer.server()) :: :ok
  def stop(watchdog) do
    if Process.alive?(watchdog) do
      try do
        GenServer.stop(watchdog, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  @impl GenServer
  def init({owner, lease_duration, metadata}) do
    {:ok, schedule(owner, lease_duration, metadata)}
  end

  @impl GenServer
  def handle_cast({:reset, lease_duration}, state) do
    # Context calls reset only after PostgreSQL confirms an extension. Replacing
    # both timers keeps the local deadline aligned with that durable lease.
    cancel_timer(state.warning_timer)
    cancel_timer(state.hard_timer)
    {:noreply, schedule(state.owner, lease_duration, state.metadata)}
  end

  @impl GenServer
  def handle_info({:lease_warning, token}, %{token: token} = state) do
    Logger.warning("Absurd task exceeded its active claim lease", state.metadata)
    {:noreply, state}
  end

  def handle_info({:lease_timeout, token}, %{token: token} = state) do
    # One lease duration produces a diagnostic warning; two is the hard safety
    # bound. Killing the disposable runner lets its claim expire and be retried.
    Logger.error(
      "Absurd task exceeded twice its active claim lease; terminating runner",
      state.metadata
    )

    Process.exit(state.owner, :kill)
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp schedule(owner, lease_duration, metadata) do
    # Timer cancellation can race with mailbox delivery. The shared token makes
    # messages from an older lease harmless through the catch-all handler above.
    token = make_ref()

    %{
      owner: owner,
      metadata: metadata,
      token: token,
      warning_timer: Process.send_after(self(), {:lease_warning, token}, lease_duration),
      hard_timer: Process.send_after(self(), {:lease_timeout, token}, lease_duration * 2)
    }
  end

  defp cancel_timer(timer), do: Process.cancel_timer(timer, async: true, info: false)
end
