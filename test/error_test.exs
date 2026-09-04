defmodule Absurd.ErrorTest do
  use ExUnit.Case, async: true

  test "maps Absurd terminal SQLSTATE values" do
    cancelled = %Postgrex.Error{postgres: %{pg_code: "AB001", message: "cancelled"}}
    failed = %Postgrex.Error{postgres: %{pg_code: "AB002", message: "failed"}}

    assert %Absurd.Error{kind: :cancelled, sqlstate: "AB001"} =
             Absurd.Error.from_exception(cancelled, :checkpoint)

    assert %Absurd.Error{kind: :failed_run, sqlstate: "AB002"} =
             Absurd.Error.from_exception(failed, :complete_run)
  end

  test "classifies only issued mutating connection failures as ambiguous" do
    disconnected = DBConnection.ConnectionError.exception("socket closed")
    checkout_timeout = DBConnection.ConnectionError.exception("queue timeout", :queue_timeout)

    assert %Absurd.Error{kind: :ambiguous} =
             Absurd.Error.from_exception(disconnected, :spawn_task, ambiguous?: true)

    assert %Absurd.Error{kind: :database} =
             Absurd.Error.from_exception(disconnected, :schema_version)

    assert %Absurd.Error{kind: :database} =
             Absurd.Error.from_exception(checkout_timeout, :spawn_task, ambiguous?: true)
  end
end
