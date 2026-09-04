defmodule Absurd.Step do
  @moduledoc """
  A concrete checkpoint handle used by the decomposed step API.

  `name` is the logical application name and `checkpoint_name` includes the
  deterministic occurrence suffix. A completed handle carries its cached
  protocol value.
  """

  @enforce_keys [:name, :checkpoint_name, :done]
  defstruct [:name, :checkpoint_name, :done, :value]

  @typedoc "A decomposed step handle."
  @type t :: %__MODULE__{
          name: String.t(),
          checkpoint_name: String.t(),
          done: boolean(),
          value: Absurd.JSON.value() | nil
        }
end
