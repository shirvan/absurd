defmodule Absurd.Failure do
  # Internal serialization boundary shared by runners and focused protocol tests.
  # Arbitrary task failures become bounded, JSON-safe diagnostics before they are
  # persisted; the original exception never becomes part of the wire protocol.
  @moduledoc false

  @maximum_name_bytes 128
  @maximum_message_bytes 4_096
  @maximum_stacktrace_bytes 8_192
  @maximum_stack_frames 20
  @ellipsis "..."

  # Kept callable so runner finalization does not own serialization policy.
  @doc false
  @spec serialize(term(), Exception.stacktrace()) :: Absurd.JSON.value()
  def serialize(reason, stacktrace \\ []) do
    %{
      "name" => reason_name(reason),
      "message" => reason_message(reason),
      "stacktrace" => format_stacktrace(stacktrace)
    }
  end

  defp reason_name(reason) when is_exception(reason) do
    reason.__struct__
    |> inspect()
    |> truncate(@maximum_name_bytes)
  end

  defp reason_name(_reason), do: "TaskError"

  defp reason_message(reason) when is_exception(reason) do
    reason
    |> Exception.message()
    |> truncate(@maximum_message_bytes)
  end

  defp reason_message(reason) do
    reason
    |> inspect(limit: 50, printable_limit: @maximum_message_bytes, width: 120)
    |> truncate(@maximum_message_bytes)
  end

  defp format_stacktrace([]), do: nil

  defp format_stacktrace(stacktrace) do
    stacktrace
    |> Enum.take(@maximum_stack_frames)
    |> Exception.format_stacktrace()
    |> truncate(@maximum_stacktrace_bytes)
  end

  defp truncate(value, maximum) do
    # Measure bytes because both storage and telemetry budgets are byte-oriented.
    # Normalize invalid strings first, then reserve room for an explicit ellipsis.
    value = ensure_valid_utf8(value, maximum)

    if byte_size(value) <= maximum do
      value
    else
      prefix_size = maximum - byte_size(@ellipsis)
      prefix = valid_prefix(value, prefix_size)
      prefix <> @ellipsis
    end
  end

  defp ensure_valid_utf8(value, maximum) do
    if String.valid?(value) do
      value
    else
      inspect(value, binaries: :as_binaries, limit: 50, printable_limit: maximum)
    end
  end

  defp valid_prefix(value, size) do
    # A byte cut may land inside a multi-byte UTF-8 code point. Walk backward to
    # the nearest valid boundary so truncation cannot create invalid diagnostics.
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix) do
      prefix
    else
      valid_prefix(value, size - 1)
    end
  end
end
