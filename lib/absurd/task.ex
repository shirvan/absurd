defmodule Absurd.Task do
  @moduledoc """
  Behaviour for application-defined durable tasks.

  Persisted task names and queue names are explicit strings; Elixir module
  names are never used as durable protocol names implicitly.

  ## Example

      defmodule MyApp.SendEmail do
        use Absurd.Task,
          name: "send-email",
          queue: "io",
          default_max_attempts: 3

        @impl Absurd.Task
        def run(params, _context), do: {:ok, params}
      end
  """

  alias Absurd.Context
  alias Absurd.JSON

  @typedoc "The explicit success or failure returned by a task callback."
  @type result :: {:ok, JSON.value()} | {:error, term()}

  @typedoc "Compile-time metadata attached to a task module."
  @type metadata :: %{
          required(:name) => String.t(),
          required(:queue) => String.t() | nil,
          required(:default_max_attempts) => pos_integer() | nil,
          required(:default_cancellation) => keyword() | nil
        }

  @doc "Runs one attempt of a durable task."
  @callback run(params :: JSON.value(), context :: Context.t()) :: result()

  @doc """
  Declares an application module as an Absurd task.

  `:name` is required. Optional registration defaults are `:queue`,
  `:default_max_attempts`, and `:default_cancellation`. A cancellation policy
  accepts `:max_duration` and `:max_delay` in seconds.
  """
  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(options) do
    metadata = validate_options!(options)

    quote do
      @behaviour Absurd.Task

      # Generated metadata is consumed by the SDK and is not application API.
      @doc false
      @spec __absurd_task__() :: Absurd.Task.metadata()
      def __absurd_task__, do: unquote(Macro.escape(metadata))
    end
  end

  defp validate_options!(options) when is_list(options) do
    allowed = [:name, :queue, :default_max_attempts, :default_cancellation]
    unknown = Keyword.keys(options) -- allowed

    if unknown != [] do
      raise ArgumentError, "unknown Absurd task options: #{inspect(unknown)}"
    end

    name = Keyword.fetch!(options, :name)
    queue = Keyword.get(options, :queue)
    max_attempts = Keyword.get(options, :default_max_attempts)
    cancellation = Keyword.get(options, :default_cancellation)

    validate_name!(:name, name)
    validate_optional_queue!(queue)
    validate_max_attempts!(max_attempts)
    validate_cancellation!(cancellation)

    %{
      name: name,
      queue: queue,
      default_max_attempts: max_attempts,
      default_cancellation: cancellation
    }
  end

  defp validate_options!(_options) do
    raise ArgumentError, "Absurd task options must be a keyword list"
  end

  defp validate_name!(:name, value) when is_binary(value) do
    if String.trim(value) == "" do
      invalid_name!(:name, value)
    end
  end

  defp validate_name!(field, value), do: invalid_name!(field, value)

  defp invalid_name!(field, value) do
    raise ArgumentError,
          "Absurd task #{field} must be a non-empty string, got: #{inspect(value)}"
  end

  defp validate_optional_queue!(nil), do: :ok

  defp validate_optional_queue!(queue) when is_binary(queue) do
    cond do
      byte_size(queue) == 0 -> invalid_name!(:queue, queue)
      byte_size(queue) > Absurd.Name.max_queue_bytes() -> raise ArgumentError, "queue is too long"
      true -> :ok
    end
  end

  defp validate_optional_queue!(queue), do: invalid_name!(:queue, queue)

  defp validate_max_attempts!(nil), do: :ok
  defp validate_max_attempts!(value) when is_integer(value) and value > 0, do: :ok

  defp validate_max_attempts!(value) do
    raise ArgumentError,
          "Absurd task default_max_attempts must be a positive integer, got: #{inspect(value)}"
  end

  defp validate_cancellation!(nil), do: :ok

  defp validate_cancellation!(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      validate_cancellation_entries!(policy)
    else
      invalid_cancellation!(policy)
    end
  end

  defp validate_cancellation!(policy), do: invalid_cancellation!(policy)

  defp validate_cancellation_entries!(policy) do
    unknown = Keyword.keys(policy) -- [:max_duration, :max_delay]

    if unknown != [] or
         Enum.any?(policy, fn {_key, value} -> not (is_integer(value) and value >= 0) end) do
      invalid_cancellation!(policy)
    end

    :ok
  end

  defp invalid_cancellation!(policy) do
    raise ArgumentError,
          "Absurd task default_cancellation must contain non-negative :max_duration and/or " <>
            ":max_delay seconds, got: #{inspect(policy)}"
  end
end
