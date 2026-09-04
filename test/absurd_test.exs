defmodule AbsurdTest do
  use ExUnit.Case, async: true

  test "is one OTP application" do
    assert Application.spec(:absurd, :mod) == {Absurd.Application, []}
    assert Process.whereis(Absurd.Supervisor)
  end

  test "builds a process-free client" do
    assert %Absurd.Client{db: MyApp.DB, queue: "default", default_max_attempts: 5} =
             Absurd.client(db: MyApp.DB)
  end
end
