defmodule Absurd.CleanupResult do
  @moduledoc """
  Counts one bounded maintenance pass for an Absurd queue.

  Cleanup policy and deletion ordering remain owned by the upstream SQL
  implementation.
  """

  @enforce_keys [:queue_name, :tasks_deleted, :events_deleted]
  defstruct @enforce_keys

  @typedoc "The deletion counts returned by `absurd.cleanup_all_queues`."
  @type t :: %__MODULE__{
          queue_name: String.t(),
          tasks_deleted: non_neg_integer(),
          events_deleted: non_neg_integer()
        }
end
