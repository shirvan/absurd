defmodule Absurd.JSON do
  @moduledoc """
  The JSON boundary shared with the Absurd PostgreSQL schema.

  Absurd stores protocol values in JSONB. Consequently, arbitrary Elixir terms,
  structs, tuples, atom map keys, and ETF values are rejected before a query is
  issued.

  ## Examples

      iex> Absurd.JSON.valid?(%{"name" => "Ada", "attempt" => 1})
      true

      iex> match?({:error, %Absurd.Error{kind: :validation}}, Absurd.JSON.validate(%{name: "Ada"}))
      true

      iex> Absurd.JSON.encode([true, nil, 3])
      {:ok, "[true,null,3]"}

  """

  alias Absurd.Error

  @typedoc "A scalar accepted by JSON and PostgreSQL JSONB."
  @type scalar :: nil | boolean() | number() | String.t()

  @typedoc "A recursively JSON-compatible Elixir value with binary object keys."
  @type value :: scalar() | [value()] | %{optional(String.t()) => value()}

  @doc "Returns whether `value` can cross the protocol JSON boundary."
  @spec valid?(term()) :: boolean()
  def valid?(value) do
    match?(:ok, validate(value))
  end

  @doc """
  Validates a protocol value.

  Returns an `Absurd.Error` describing the first invalid value and its path.
  """
  @spec validate(term()) :: :ok | {:error, Error.t()}
  def validate(value) do
    with :ok <- validate_structure(value, []),
         {:ok, _encoded} <- encode_value(value) do
      :ok
    end
  end

  @doc "Validates and encodes a protocol value with Jason."
  @spec encode(term()) :: {:ok, String.t()} | {:error, Error.t()}
  def encode(value) do
    with :ok <- validate_structure(value, []) do
      encode_value(value)
    end
  end

  defp validate_structure(value, _path)
       when is_nil(value) or is_boolean(value) or is_integer(value) or is_float(value) or
              is_binary(value),
       do: :ok

  defp validate_structure([], _path), do: :ok

  defp validate_structure([head | tail], path) do
    with :ok <- validate_structure(head, [0 | path]) do
      validate_list_tail(tail, path, 1)
    end
  end

  defp validate_structure(value, path) when is_map(value) and not is_struct(value) do
    Enum.reduce_while(value, :ok, fn
      {key, nested}, :ok when is_binary(key) ->
        case validate_structure(nested, [key | path]) do
          :ok -> {:cont, :ok}
          {:error, _error} = error -> {:halt, error}
        end

      {key, _nested}, :ok ->
        {:halt, invalid("JSON object keys must be strings", [key | path])}
    end)
  end

  defp validate_structure(_value, path) do
    invalid("value is not JSON-compatible", path)
  end

  defp validate_list_tail([], _path, _index), do: :ok

  defp validate_list_tail([head | tail], path, index) do
    with :ok <- validate_structure(head, [index | path]) do
      validate_list_tail(tail, path, index + 1)
    end
  end

  defp validate_list_tail(_improper_tail, path, index) do
    invalid("JSON arrays must be proper lists", [index | path])
  end

  defp encode_value(value) do
    case Jason.encode(value) do
      {:ok, encoded} ->
        {:ok, encoded}

      {:error, exception} ->
        {:error,
         Error.new(:validation, "value cannot be encoded as JSON",
           cause: exception,
           metadata: %{field: :json}
         )}
    end
  end

  defp invalid(message, path) do
    {:error, Error.new(:validation, message, metadata: %{field: :json, path: Enum.reverse(path)})}
  end
end
