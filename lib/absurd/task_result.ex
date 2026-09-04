defmodule Absurd.TaskResult do
  @moduledoc """
  A durable snapshot of a task's current state.

  Only completed tasks carry `:result`; only failed tasks carry `:failure`.

  ## Examples

      iex> snapshot = %Absurd.TaskResult{state: :running, result: nil, failure: nil}
      iex> Absurd.TaskResult.terminal?(snapshot)
      false

      iex> snapshot = %Absurd.TaskResult{state: :cancelled, result: nil, failure: nil}
      iex> Absurd.TaskResult.terminal?(snapshot)
      true

  """

  @enforce_keys [:state, :result, :failure]
  defstruct [:state, :result, :failure]

  @typedoc "A task state defined by the upstream Absurd schema."
  @type state :: :pending | :running | :sleeping | :completed | :failed | :cancelled

  @typedoc "A task result snapshot."
  @type t :: %__MODULE__{
          state: state(),
          result: Absurd.JSON.value() | nil,
          failure: Absurd.JSON.value() | nil
        }

  @doc "Returns whether a snapshot has reached a durable terminal state."
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{state: state}) do
    state in [:completed, :failed, :cancelled]
  end
end
