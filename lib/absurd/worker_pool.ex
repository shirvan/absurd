defmodule Absurd.WorkerPool do
  @moduledoc """
  OTP supervision boundary for workers consuming one Absurd queue.

  A pool validates its task catalog and the installed Absurd schema before it
  starts claiming. It owns an unnamed dynamic supervisor for temporary runners
  and one capacity-aware poller; the caller continues to own the Postgrex
  queryable.

  Place the pool after that queryable in the host application's supervision
  tree:

      children = [
        {Postgrex, name: MyApp.AbsurdDB, database: "my_app"},
        {Absurd.WorkerPool,
         name: MyApp.AbsurdWorkers,
         db: MyApp.AbsurdDB,
         queue: "default",
         tasks: [MyApp.SendEmail]}
      ]

  Runner processes are temporary. PostgreSQL, rather than OTP restart policy,
  decides whether interrupted durable work is retried.
  """

  use Supervisor

  alias Absurd.Error
  alias Absurd.Poller
  alias Absurd.SQL
  alias Absurd.TaskCatalog

  @runner_supervisor_id {__MODULE__, :runner_supervisor}
  @required_options [:name, :db, :queue, :tasks]
  @positive_options [:concurrency, :batch_size, :claim_timeout, :poll_interval, :shutdown]
  @option_defaults [
    name: nil,
    db: nil,
    queue: nil,
    tasks: nil,
    concurrency: 1,
    batch_size: nil,
    claim_timeout: 120_000,
    poll_interval: 250,
    shutdown: 30_000,
    worker_id: nil,
    hooks: nil,
    query_options: []
  ]

  @doc """
  Starts a validated worker pool.

  Required options are:

    * `:name` - an OTP server name for the pool;
    * `:db` - a caller-owned Postgrex queryable;
    * `:queue` - the queue consumed by this pool;
    * `:tasks` - task modules or an `Absurd.TaskCatalog` behaviour module.

  Optional values are `:concurrency` (default `1`), `:batch_size` (defaulting
  to concurrency), `:claim_timeout` (`120_000` milliseconds),
  `:poll_interval` (`250` milliseconds), `:shutdown` (`30_000` milliseconds),
  `:worker_id` (default `hostname:os-pid`), `:hooks` (`Absurd.Hooks` module),
  and Postgrex `:query_options`.

  Startup fails before supervision or claims if options, the catalog, or
  `Absurd.SQL.verify_schema_version/2` are invalid.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options) do
    # Finish every fallible configuration check before starting supervision. A
    # pool that cannot dispatch its catalog or understand the installed schema
    # must never begin claiming durable work.
    with {:ok, validated} <- validate_options(options),
         {:ok, catalog} <- TaskCatalog.new(validated[:tasks], validated[:queue]),
         :ok <- SQL.verify_schema_version(validated[:db], validated[:query_options]) do
      supervisor_options = Keyword.put(validated, :catalog, catalog)
      Supervisor.start_link(__MODULE__, supervisor_options, name: validated[:name])
    end
  end

  @impl Supervisor
  def init(options) do
    # Child order is part of the lifecycle contract. The poller needs the runner
    # supervisor to exist before it claims, and supervisor shutdown happens in
    # reverse order so the poller can drain runners before their owner stops.
    runner_supervisor = %{
      id: @runner_supervisor_id,
      start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
      type: :supervisor,
      shutdown: :infinity
    }

    poller_options =
      options
      |> Keyword.drop([:name, :tasks])
      |> Keyword.put(:pool, self())
      |> Keyword.put(:runner_supervisor_id, @runner_supervisor_id)

    # :rest_for_one couples capacity accounting to the process it accounts for:
    # if the runner supervisor dies, restart the poller too; if only the poller
    # dies, preserve live runners and let its replacement monitor them again.
    Supervisor.init([runner_supervisor, {Poller, poller_options}], strategy: :rest_for_one)
  end

  @doc "Returns an OTP child specification keyed by the configured pool name."
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(options) do
    name = Keyword.fetch!(options, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  defp validate_options(options) when is_list(options) do
    if Keyword.keyword?(options) do
      case Keyword.validate(options, @option_defaults) do
        {:ok, validated} ->
          validate_values(validated)

        {:error, unknown} ->
          configuration_error("unknown worker-pool options", %{options: unknown})
      end
    else
      configuration_error("worker-pool options must be a keyword list")
    end
  end

  defp validate_options(_options) do
    configuration_error("worker-pool options must be a keyword list")
  end

  defp validate_values(options) do
    # A missing batch size means "claim up to this pool's concurrency." Normalize
    # it once so the poller operates on concrete values only.
    concurrency = options[:concurrency]
    batch_size = options[:batch_size] || concurrency

    options =
      options
      |> Keyword.put(:batch_size, batch_size)
      |> Keyword.put(:worker_id, options[:worker_id] || default_worker_id())

    with :ok <- validate_required_options(options),
         :ok <- validate_name(options[:name]),
         :ok <- validate_db(options[:db]),
         :ok <- validate_queue(options[:queue]),
         :ok <- validate_tasks(options[:tasks]),
         :ok <- validate_positive_options(options),
         :ok <- validate_worker_id(options[:worker_id]),
         :ok <- validate_hooks(options[:hooks]),
         :ok <- validate_query_options(options[:query_options]) do
      # Database leases have whole-second resolution. Round upward so the local
      # watchdog never expires before the lease PostgreSQL actually records.
      {:ok, Keyword.update!(options, :claim_timeout, &effective_duration/1)}
    end
  end

  defp validate_required_options(options) do
    case Enum.find(@required_options, &is_nil(options[&1])) do
      nil -> :ok
      missing -> configuration_error("missing required worker-pool option", %{field: missing})
    end
  end

  defp validate_name(name) do
    if valid_name?(name),
      do: :ok,
      else: configuration_error(":name must be an OTP server name", %{field: :name})
  end

  defp validate_db(nil), do: configuration_error(":db is required", %{field: :db})
  defp validate_db(_db), do: :ok

  defp validate_queue(queue) do
    case Absurd.Name.validate_queue(queue) do
      {:ok, _queue} -> :ok
      {:error, %Error{}} -> configuration_error(":queue is invalid", %{field: :queue})
    end
  end

  defp validate_tasks(tasks) do
    if is_list(tasks) or is_atom(tasks),
      do: :ok,
      else: configuration_error(":tasks must be a list or catalog module", %{field: :tasks})
  end

  defp validate_positive_options(options) do
    case Enum.find(@positive_options, &(not positive_integer?(options[&1]))) do
      nil ->
        :ok

      field ->
        configuration_error(":#{field} must be a positive integer", %{
          field: field,
          value: options[field]
        })
    end
  end

  defp validate_worker_id(worker_id) when is_binary(worker_id) and byte_size(worker_id) > 0,
    do: :ok

  defp validate_worker_id(_worker_id) do
    configuration_error(":worker_id must be a non-empty string", %{field: :worker_id})
  end

  defp validate_hooks(nil), do: :ok

  defp validate_hooks(hooks) when is_atom(hooks) do
    case Code.ensure_loaded(hooks) do
      {:module, ^hooks} ->
        :ok

      {:error, reason} ->
        configuration_error(":hooks module could not be loaded", %{cause: reason})
    end
  end

  defp validate_hooks(_hooks) do
    configuration_error(":hooks must be a module", %{field: :hooks})
  end

  defp validate_query_options(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: :ok,
      else: configuration_error(":query_options must be a keyword list", %{field: :query_options})
  end

  defp validate_query_options(_options) do
    configuration_error(":query_options must be a keyword list", %{field: :query_options})
  end

  defp valid_name?(name), do: is_atom(name) or match?({:global, _term}, name) or valid_via?(name)

  defp valid_via?({:via, module, _term}), do: is_atom(module)
  defp valid_via?(_name), do: false

  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp effective_duration(duration), do: div(duration + 999, 1_000) * 1_000

  defp default_worker_id do
    hostname =
      case :inet.gethostname() do
        {:ok, hostname} -> List.to_string(hostname)
        {:error, _reason} -> "host"
      end

    hostname <> ":" <> System.pid()
  end

  defp configuration_error(message, metadata \\ %{}) do
    {:error,
     Error.new(:configuration, message, operation: :start_worker_pool, metadata: metadata)}
  end
end
