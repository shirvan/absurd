defmodule Absurd.Supervisor do
  @moduledoc """
  Root supervisor for configured Absurd worker pools.

  Client operations do not depend on this supervisor. When no worker pools are
  configured, it deliberately has no children.
  """

  use Supervisor

  @doc "Starts the root supervisor for the supplied `:worker_pools`."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options) do
    Supervisor.start_link(__MODULE__, options, name: __MODULE__)
  end

  @impl Supervisor
  def init(options) do
    children =
      options
      |> Keyword.fetch!(:worker_pools)
      |> Enum.map(&worker_pool_child_spec/1)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp worker_pool_child_spec(options) do
    name = Keyword.fetch!(options, :name)

    # Key children by their configured OTP name so multiple queues/pools can use
    # the same module without colliding on Supervisor's default module id.
    Supervisor.child_spec(
      {Absurd.WorkerPool, options},
      id: {Absurd.WorkerPool, name}
    )
  end
end
