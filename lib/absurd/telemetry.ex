defmodule Absurd.Telemetry do
  @moduledoc """
  Telemetry contract for Absurd database calls and task execution.

  SQL operations emit `[:absurd, :sql, operation, phase]`. Runner executions
  emit `[:absurd, :runner, :execute, phase]`. The final element is `:start`,
  `:stop`, or `:exception`.

  Start measurements contain `:system_time`; stop and exception measurements
  contain monotonic `:duration`. Metadata identifies stable operations and
  durable names but never includes task parameters, results, headers, or
  exception values. Telemetry reports observed SDK behavior and is not evidence
  that an ambiguous database transaction committed.

  Attach handlers to the concrete operations and phases an application needs:

      :telemetry.attach_many(
        "my-app-absurd",
        [
          [:absurd, :sql, :spawn_task, :stop],
          [:absurd, :runner, :execute, :stop],
          [:absurd, :runner, :execute, :exception]
        ],
        &MyApp.AbsurdTelemetry.handle_event/4,
        nil
      )

  ## Examples

      iex> Absurd.Telemetry.event_prefix()
      [:absurd]

  """

  alias Absurd.Error

  @event_prefix [:absurd]
  @known_outcomes [
    :completed,
    :failed,
    :suspended,
    :cancelled,
    :already_failed,
    :deferred,
    :ambiguous,
    :unfinalized
  ]
  @ellipsis "..."

  @doc "Returns the common prefix used by every SDK telemetry event."
  @spec event_prefix() :: [atom()]
  def event_prefix, do: @event_prefix

  # Internal span primitive shared by the SQL and runner boundaries.
  @doc false
  @spec span(atom(), atom(), map(), (-> term())) :: term()
  def span(component, operation, metadata, function)
      when is_atom(component) and is_atom(operation) and is_map(metadata) and
             is_function(function, 0) do
    started_at = System.monotonic_time()
    metadata = Map.merge(metadata, %{component: component, operation: operation})
    event = @event_prefix ++ [component, operation]

    :telemetry.execute(event ++ [:start], %{system_time: System.system_time()}, metadata)
    execute_span(event, metadata, started_at, function)
  end

  # Internal cardinality guard used before durable names enter logs or telemetry.
  @doc false
  @spec bounded_string(String.t(), pos_integer()) :: String.t()
  def bounded_string(value, maximum) when is_binary(value) and maximum >= byte_size(@ellipsis) do
    value = ensure_valid_utf8(value, maximum)

    if byte_size(value) <= maximum do
      value
    else
      prefix_size = maximum - byte_size(@ellipsis)
      valid_prefix(value, prefix_size) <> @ellipsis
    end
  end

  defp execute_span(event, metadata, started_at, function) do
    # Emit exactly one terminal event. Tagged error returns are ordinary stops;
    # raised, thrown, and exited control flow is observed as an exception and then
    # re-raised unchanged so instrumentation cannot alter program semantics.
    result = function.()
    measurements = %{duration: System.monotonic_time() - started_at}
    stop_metadata = Map.merge(metadata, outcome_metadata(result))
    :telemetry.execute(event ++ [:stop], measurements, stop_metadata)
    result
  rescue
    exception ->
      stacktrace = __STACKTRACE__
      emit_exception(event, metadata, started_at, exception.__struct__)
      reraise exception, stacktrace
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__
      emit_exception(event, metadata, started_at, kind)
      :erlang.raise(kind, reason, stacktrace)
  end

  defp emit_exception(event, metadata, started_at, error_kind) do
    measurements = %{duration: System.monotonic_time() - started_at}
    exception_metadata = Map.merge(metadata, %{outcome: :exception, error_kind: error_kind})
    :telemetry.execute(event ++ [:exception], measurements, exception_metadata)
  end

  defp outcome_metadata(:ok), do: %{outcome: :success}
  defp outcome_metadata({:ok, _value}), do: %{outcome: :success}

  defp outcome_metadata({:error, %Error{kind: kind}}) do
    %{outcome: :error, error_kind: kind}
  end

  defp outcome_metadata({:error, _reason}), do: %{outcome: :error}

  defp outcome_metadata(outcome) when outcome in @known_outcomes do
    %{outcome: outcome}
  end

  defp outcome_metadata(_result), do: %{outcome: :success}

  defp ensure_valid_utf8(value, maximum) do
    # Durable names are application input. Bound and sanitize them before they
    # become log/metric dimensions, without ever attaching params or results.
    if String.valid?(value) do
      value
    else
      inspect(value, binaries: :as_binaries, limit: 50, printable_limit: maximum)
    end
  end

  defp valid_prefix(value, size) do
    # Do not split a multi-byte character when enforcing the byte budget.
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix) do
      prefix
    else
      valid_prefix(value, size - 1)
    end
  end
end
