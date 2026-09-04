defmodule Absurd.Error do
  @moduledoc """
  The public error returned by Absurd operations.

  Errors keep a stable `:kind` and human-readable `:message` while retaining
  bounded diagnostic context. Database errors may also expose their SQLSTATE
  without requiring callers to match on Postgrex internals.

  ## Examples

      iex> error = Absurd.Error.new(:validation, "queue must not be empty")
      iex> {error.kind, Exception.message(error)}
      {:validation, "queue must not be empty"}

  """

  @enforce_keys [:kind, :message]
  defexception [:kind, :message, :operation, :sqlstate, :cause, metadata: %{}]

  @typedoc "The stable category of an Absurd error."
  @type kind ::
          :validation
          | :configuration
          | :database
          | :ambiguous
          | :protocol
          | :schema_incompatible
          | :cancelled
          | :failed_run
          | :timeout
          | :unknown_task

  @typedoc "A public Absurd error value."
  @type t :: %__MODULE__{
          kind: kind(),
          message: String.t(),
          operation: atom() | nil,
          sqlstate: String.t() | nil,
          cause: term(),
          metadata: map()
        }

  @doc """
  Builds an error with optional diagnostic fields.

  Supported options are `:operation`, `:sqlstate`, `:cause`, and `:metadata`.
  """
  @spec new(kind(), String.t(), keyword()) :: t()
  def new(kind, message, options \\ []) when is_atom(kind) and is_binary(message) do
    %__MODULE__{
      kind: kind,
      message: message,
      operation: Keyword.get(options, :operation),
      sqlstate: Keyword.get(options, :sqlstate),
      cause: Keyword.get(options, :cause),
      metadata: Keyword.get(options, :metadata, %{})
    }
  end

  @doc """
  Converts a database exception into the public error envelope.

  Absurd SQLSTATE `AB001` is classified as `:cancelled`; `AB002` is classified
  as `:failed_run`. Pass `ambiguous?: true` for a mutating operation whose
  established connection was lost before its outcome was observed. Connection
  checkout timeouts are never ambiguous because no query was issued.
  """
  @spec from_exception(Exception.t(), atom() | nil, keyword()) :: t()
  def from_exception(exception, operation \\ nil, options \\ []) do
    sqlstate = sqlstate(exception)

    new(kind_for_exception(exception, sqlstate, options), Exception.message(exception),
      operation: operation,
      sqlstate: sqlstate,
      cause: exception
    )
  end

  defp sqlstate(%Postgrex.Error{postgres: postgres}) when is_map(postgres) do
    postgres[:pg_code]
  end

  defp sqlstate(_exception), do: nil

  defp kind_for_exception(_exception, "AB001", _options), do: :cancelled
  defp kind_for_exception(_exception, "AB002", _options), do: :failed_run

  defp kind_for_exception(
         %DBConnection.ConnectionError{reason: reason},
         _sqlstate,
         options
       )
       when reason != :queue_timeout do
    if Keyword.get(options, :ambiguous?, false), do: :ambiguous, else: :database
  end

  defp kind_for_exception(_exception, _sqlstate, _options), do: :database
end
