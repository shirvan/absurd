defmodule Absurd.SpawnResult do
  @moduledoc """
  Identifies a task and the run returned by spawn or retry.

  `created` is `false` when an idempotency key resolves to an existing task or
  when a failed task is retried in place.
  """

  @enforce_keys [:queue, :task_id, :run_id, :attempt, :created]
  defstruct [:queue, :task_id, :run_id, :attempt, :created]

  @typedoc "The durable identity returned by a spawn or retry operation."
  @type t :: %__MODULE__{
          queue: String.t(),
          task_id: binary(),
          run_id: binary(),
          attempt: pos_integer(),
          created: boolean()
        }
end
