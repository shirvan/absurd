defmodule Absurd.TelemetryPostgreSQLTest do
  use Absurd.PostgreSQLCase

  alias Absurd.Client

  test "SQL telemetry reports the validated queue and observed outcome", context do
    event = [:absurd, :sql, :get_queue_policy, :stop]
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        &Absurd.TestTelemetryHandler.handle_event/4,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, _policy} = Client.get_queue_policy(context.client)

    assert_receive {^event, %{duration: duration}, metadata}, 1_000
    assert is_integer(duration)
    assert metadata.component == :sql
    assert metadata.operation == :get_queue_policy
    assert metadata.queue == context.queue
    assert metadata.outcome == :success
  end
end
