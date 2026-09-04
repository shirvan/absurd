defmodule Absurd.Hooks do
  @moduledoc """
  Optional lifecycle hooks for context propagation and tracing integrations.

  Hook modules may enrich spawn headers or wrap task execution. They must not
  reinterpret Absurd's private suspension, cancellation, or finalization
  control flow.

  ## Example

      defmodule MyApp.AbsurdHooks do
        @behaviour Absurd.Hooks

        @impl Absurd.Hooks
        def before_spawn(_task_name, _params, options) do
          headers = Keyword.get(options, :headers) || %{}
          headers = Map.put(headers, "source", "my-app")
          {:ok, Keyword.put(options, :headers, headers)}
        end

        @impl Absurd.Hooks
        def wrap_task_execution(context, execute) do
          Logger.metadata(absurd_task: context.task_name, attempt: context.attempt)
          execute.()
        end
      end

  Configure the module independently on `Absurd.Client` for `before_spawn/3`
  and on `Absurd.WorkerPool` for `wrap_task_execution/2`.
  """

  alias Absurd.Context
  alias Absurd.JSON
  alias Absurd.Task

  @doc "May validate or enrich normalized options immediately before spawn."
  @callback before_spawn(task_name :: String.t(), params :: JSON.value(), options :: keyword()) ::
              {:ok, keyword()} | {:error, term()}

  @doc "Wraps one task callback execution and returns its ordinary result."
  @callback wrap_task_execution(context :: Context.t(), execute :: (-> Task.result())) ::
              Task.result()

  @optional_callbacks before_spawn: 3, wrap_task_execution: 2
end
