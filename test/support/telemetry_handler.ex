defmodule Absurd.TestTelemetryHandler do
  @moduledoc false

  @spec handle_event([atom()], map(), map(), pid()) :: :ok
  def handle_event(event, measurements, metadata, test_pid) do
    send(test_pid, {event, measurements, metadata})
    :ok
  end
end
