defmodule Absurd.LeaseWatchdogTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Absurd.LeaseWatchdog

  test "kills only its owner after twice the lease" do
    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_ref = Process.monitor(owner)

    log =
      capture_log(fn ->
        assert {:ok, _watchdog} = LeaseWatchdog.start_link(owner, 20, %{queue: "test"})
        assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, 250
      end)

    assert log =~ "exceeded its active claim lease"
    assert log =~ "terminating runner"
    assert Process.alive?(self())
  end

  test "reset replaces both lease timers" do
    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_ref = Process.monitor(owner)
    assert {:ok, watchdog} = LeaseWatchdog.start_link(owner, 20, %{})

    assert :ok = LeaseWatchdog.reset(watchdog, 200)
    refute_receive {:DOWN, ^owner_ref, :process, ^owner, _reason}, 80
    assert :ok = LeaseWatchdog.stop(watchdog)

    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}
  end

  test "ignores timer messages from a lease replaced by reset" do
    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_ref = Process.monitor(owner)
    assert {:ok, watchdog} = LeaseWatchdog.start_link(owner, 200, %{})

    old_token = :sys.get_state(watchdog).token
    assert :ok = LeaseWatchdog.reset(watchdog, 200)
    new_token = :sys.get_state(watchdog).token
    refute new_token == old_token

    send(watchdog, {:lease_warning, old_token})
    send(watchdog, {:lease_timeout, old_token})

    refute_receive {:DOWN, ^owner_ref, :process, ^owner, _reason}, 50
    assert Process.alive?(watchdog)
    assert :ok = LeaseWatchdog.stop(watchdog)

    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}
  end
end
