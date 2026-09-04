defmodule Absurd.EventWait do
  @moduledoc """
  The durable decision returned when a run awaits an event.

  When `should_suspend` is true, the database has put the run to sleep and the
  runner must stop without completing or failing it. Otherwise `payload` is the
  resolved event value, or `nil` for an elapsed timeout.
  """

  @enforce_keys [:should_suspend, :payload]
  defstruct @enforce_keys

  @typedoc "An event wait decision returned by the upstream SQL protocol."
  @type t :: %__MODULE__{
          should_suspend: boolean(),
          payload: Absurd.JSON.value() | nil
        }
end
