defmodule Absurd.Name do
  @moduledoc """
  Validation for names persisted by the Absurd protocol.

  Queue names follow the upstream SQL rule: they may contain any characters,
  including whitespace, but must not be empty and must fit in 57 UTF-8 bytes.
  Task, step, and event names must contain at least one non-whitespace
  character.

  ## Examples

      iex> Absurd.Name.validate_queue("Queue Name-1")
      {:ok, "Queue Name-1"}

      iex> match?({:error, %Absurd.Error{kind: :validation}}, Absurd.Name.validate_queue(String.duplicate("é", 29)))
      true

      iex> Absurd.Name.validate_durable("send-email", :task)
      {:ok, "send-email"}

  """

  alias Absurd.Error

  @max_queue_bytes 57

  @doc "Returns the maximum encoded byte length accepted for queue names."
  @spec max_queue_bytes() :: pos_integer()
  def max_queue_bytes, do: @max_queue_bytes

  @doc """
  Validates a queue name without changing it.

  Whitespace-only queue names are valid because upstream deliberately delegates
  the character set to PostgreSQL's quoted-identifier rules.
  """
  @spec validate_queue(term()) :: {:ok, String.t()} | {:error, Error.t()}
  def validate_queue(name) when is_binary(name) do
    cond do
      not String.valid?(name) ->
        {:error, Error.new(:validation, "queue must be valid UTF-8", metadata: %{field: :queue})}

      name == "" ->
        {:error, queue_required_error()}

      byte_size(name) > @max_queue_bytes ->
        {:error,
         Error.new(:validation, "queue is too long (maximum #{@max_queue_bytes} UTF-8 bytes)",
           metadata: %{field: :queue, max_bytes: @max_queue_bytes}
         )}

      true ->
        {:ok, name}
    end
  end

  def validate_queue(_name) do
    {:error, queue_required_error()}
  end

  @doc """
  Validates a task, step, or event name without changing it.

  The `field` identifies the value in a returned validation error.
  """
  @spec validate_durable(term(), :task | :step | :event) ::
          {:ok, String.t()} | {:error, Error.t()}
  def validate_durable(name, field)
      when field in [:task, :step, :event] and is_binary(name) do
    cond do
      not String.valid?(name) ->
        {:error,
         Error.new(:validation, "#{field} name must be valid UTF-8", metadata: %{field: field})}

      String.trim(name) == "" ->
        {:error, durable_name_error(field)}

      true ->
        {:ok, name}
    end
  end

  def validate_durable(_name, field) when field in [:task, :step, :event] do
    {:error, durable_name_error(field)}
  end

  defp durable_name_error(field) do
    Error.new(:validation, "#{field} name must be a non-empty string", metadata: %{field: field})
  end

  defp queue_required_error do
    Error.new(:validation, "queue must be a non-empty string", metadata: %{field: :queue})
  end
end
