defmodule Absurd.TaskTest do
  use ExUnit.Case, async: true

  defmodule ExampleTask do
    use Absurd.Task,
      name: "example",
      queue: "default",
      default_max_attempts: 3,
      default_cancellation: [max_duration: 60]

    @impl Absurd.Task
    def run(params, _context), do: {:ok, params}
  end

  test "uses explicit durable names" do
    assert ExampleTask.__absurd_task__() == %{
             name: "example",
             queue: "default",
             default_max_attempts: 3,
             default_cancellation: [max_duration: 60]
           }
  end
end
