defmodule Absurd.TelemetryTest do
  use ExUnit.Case, async: true

  alias Absurd.Error
  alias Absurd.Telemetry

  test "emits bounded start and stop events without payload data" do
    events = [
      [:absurd, :sql, :example, :start],
      [:absurd, :sql, :example, :stop]
    ]

    attach(events)
    secret_result = %{"secret" => "not telemetry"}

    assert {:error, %Error{kind: :database}} =
             Telemetry.span(:sql, :example, %{queue: "default"}, fn ->
               {:error, Error.new(:database, "offline", cause: secret_result)}
             end)

    assert_receive {[:absurd, :sql, :example, :start], %{system_time: system_time}, start}
    assert is_integer(system_time)
    assert start == %{component: :sql, operation: :example, queue: "default"}

    assert_receive {[:absurd, :sql, :example, :stop], %{duration: duration}, stop}
    assert is_integer(duration)
    assert stop.outcome == :error
    assert stop.error_kind == :database
    refute inspect(stop) =~ "not telemetry"
  end

  test "exception events identify the kind without exposing the exception" do
    events = [
      [:absurd, :runner, :execute, :start],
      [:absurd, :runner, :execute, :exception]
    ]

    attach(events)

    assert_raise RuntimeError, "private failure", fn ->
      Telemetry.span(:runner, :execute, %{}, fn -> raise "private failure" end)
    end

    assert_receive {[:absurd, :runner, :execute, :start], _measurements, _metadata}

    assert_receive {[:absurd, :runner, :execute, :exception], %{duration: duration}, metadata}

    assert is_integer(duration)
    assert metadata.outcome == :exception
    assert metadata.error_kind == RuntimeError
    refute Map.has_key?(metadata, :reason)
    refute Map.has_key?(metadata, :stacktrace)
    refute inspect(metadata) =~ "private failure"
  end

  test "bounds UTF-8 metadata without splitting a codepoint" do
    value = String.duplicate("é", 100)
    bounded = Telemetry.bounded_string(value, 32)

    assert byte_size(bounded) <= 32
    assert String.valid?(bounded)
    assert String.ends_with?(bounded, "...")
  end

  defp attach(events) do
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &Absurd.TestTelemetryHandler.handle_event/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
