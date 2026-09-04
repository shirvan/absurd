defmodule Absurd.QueuePolicy do
  @moduledoc """
  Maintenance policy returned for an Absurd queue.

  Interval fields remain PostgreSQL interval strings, matching the official
  TypeScript and Go SDK surface without exposing driver-specific interval
  structs.
  """

  @enforce_keys [
    :queue_name,
    :storage_mode,
    :partition_lookahead,
    :partition_lookback,
    :cleanup_ttl,
    :cleanup_limit,
    :detach_mode,
    :detach_min_age
  ]
  defstruct @enforce_keys

  @typedoc "The table-storage mode of a queue."
  @type storage_mode :: :unpartitioned | :partitioned

  @typedoc "The maintenance detach behavior of a queue."
  @type detach_mode :: :none | :empty

  @typedoc "A queue's persisted maintenance policy."
  @type t :: %__MODULE__{
          queue_name: String.t(),
          storage_mode: storage_mode(),
          partition_lookahead: String.t(),
          partition_lookback: String.t(),
          cleanup_ttl: String.t(),
          cleanup_limit: pos_integer(),
          detach_mode: detach_mode(),
          detach_min_age: String.t()
        }
end
