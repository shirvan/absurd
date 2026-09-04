defmodule Absurd.TaskResultTest do
  use ExUnit.Case, async: true

  test "only completed, failed, and cancelled snapshots are terminal" do
    for state <- [:pending, :running, :sleeping] do
      refute Absurd.TaskResult.terminal?(%Absurd.TaskResult{
               state: state,
               result: nil,
               failure: nil
             })
    end

    for state <- [:completed, :failed, :cancelled] do
      assert Absurd.TaskResult.terminal?(%Absurd.TaskResult{
               state: state,
               result: nil,
               failure: nil
             })
    end
  end
end
