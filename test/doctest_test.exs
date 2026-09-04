defmodule Absurd.DoctestTest do
  use ExUnit.Case, async: true

  doctest Absurd
  doctest Absurd.Application
  doctest Absurd.Checkpoint
  doctest Absurd.ClaimedTask
  doctest Absurd.CleanupResult
  doctest Absurd.Client
  doctest Absurd.Context
  doctest Absurd.Error
  doctest Absurd.EventWait
  doctest Absurd.Hooks
  doctest Absurd.JSON
  doctest Absurd.Name
  doctest Absurd.QueuePolicy
  doctest Absurd.SpawnResult
  doctest Absurd.SQL
  doctest Absurd.Step
  doctest Absurd.Supervisor
  doctest Absurd.Task
  doctest Absurd.TaskCatalog
  doctest Absurd.TaskResult
  doctest Absurd.Telemetry
  doctest Absurd.WorkerPool
end
