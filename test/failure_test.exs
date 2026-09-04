defmodule Absurd.FailureTest do
  use ExUnit.Case, async: true

  alias Absurd.Failure

  test "serializes exceptions into bounded JSON-compatible fields" do
    exception = RuntimeError.exception(String.duplicate("é", 5_000))

    stacktrace =
      Enum.map(1..30, fn line -> {__MODULE__, :example, 0, [file: ~c"test.exs", line: line]} end)

    assert %{
             "name" => "RuntimeError",
             "message" => message,
             "stacktrace" => stacktrace_text
           } = Failure.serialize(exception, stacktrace)

    assert byte_size(message) <= 4_096
    assert byte_size(stacktrace_text) <= 8_192
    assert String.valid?(message)
    assert String.ends_with?(message, "...")
    assert :ok = Absurd.JSON.validate(Failure.serialize(exception, stacktrace))
  end

  test "uses bounded inspect output for returned failure reasons" do
    reason = {:provider_error, Enum.to_list(1..10_000)}

    assert %{
             "name" => "TaskError",
             "message" => message,
             "stacktrace" => nil
           } = Failure.serialize(reason)

    assert byte_size(message) <= 4_096
    assert :ok = Absurd.JSON.validate(Failure.serialize(reason))
  end
end
