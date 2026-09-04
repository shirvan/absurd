defmodule Absurd.ClaimedTask do
  @moduledoc """
  A run claimed from an Absurd queue for temporary execution.

  This value mirrors the fixed columns returned by `absurd.claim_task`. Queue
  ownership remains with the worker pool that issued the claim.
  """

  @enforce_keys [
    :run_id,
    :task_id,
    :task_name,
    :attempt,
    :params,
    :retry_strategy,
    :max_attempts,
    :headers,
    :wake_event,
    :event_payload
  ]
  defstruct @enforce_keys

  @typedoc "A claimed task row returned by the upstream SQL protocol."
  @type t :: %__MODULE__{
          run_id: binary(),
          task_id: binary(),
          task_name: String.t(),
          attempt: pos_integer(),
          params: Absurd.JSON.value(),
          retry_strategy: Absurd.JSON.value() | nil,
          max_attempts: pos_integer() | nil,
          headers: %{optional(String.t()) => Absurd.JSON.value()} | nil,
          wake_event: String.t() | nil,
          event_payload: Absurd.JSON.value() | nil
        }
end
