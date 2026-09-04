defmodule Absurd.Application do
  @moduledoc """
  Starts the optional Absurd worker supervision tree.

  Client calls do not depend on this process. With the default empty
  `:worker_pools` configuration, the application starts only
  `Absurd.Supervisor` and owns no database connection or worker.
  """

  use Application

  @impl Application
  def start(_type, _args) do
    worker_pools = Application.get_env(:absurd, :worker_pools, [])
    Absurd.Supervisor.start_link(worker_pools: worker_pools)
  end
end
