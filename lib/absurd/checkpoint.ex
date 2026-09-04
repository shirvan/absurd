defmodule Absurd.Checkpoint do
  @moduledoc """
  A persisted checkpoint visible to a task run.

  The database controls visibility across attempts. The Elixir layer preserves
  the returned checkpoint name, state, status, owner run, and timestamp.
  """

  @enforce_keys [:checkpoint_name, :state, :status, :owner_run_id, :updated_at]
  defstruct @enforce_keys

  @typedoc "A checkpoint row returned by the upstream SQL protocol."
  @type t :: %__MODULE__{
          checkpoint_name: String.t(),
          state: Absurd.JSON.value(),
          status: String.t(),
          owner_run_id: binary(),
          updated_at: DateTime.t()
        }
end
