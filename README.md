# Absurd SDK for Elixir

[![CI](https://github.com/shirvan/absurd/actions/workflows/ci.yml/badge.svg)](https://github.com/shirvan/absurd/actions/workflows/ci.yml)

An unofficial, community-maintained Elixir SDK for
[Absurd](https://github.com/earendil-works/absurd). It provides a process-free
client, durable task helpers, and OTP-supervised workers.

## Compatibility

| Elixir SDK | Absurd schema | Elixir / OTP | Release verification |
|---|---|---|---|
| `0.2.x` | [`0.5.0`](https://github.com/earendil-works/absurd/tree/0.5.0) | Elixir 1.18+ / OTP 27+ | PostgreSQL 16; OTP 27, 28, and 29 |
| `0.1.x` | [`0.5.0`](https://github.com/earendil-works/absurd/tree/0.5.0) | Elixir 1.18+ / OTP 27+ | PostgreSQL 16; OTP 27, 28, and 29 |

The package version and Absurd version are independent: SDK `0.2.x` supports
exactly the Absurd `0.5.0` schema, as did the initial `0.1.x` release. A worker
pool verifies that schema version before it starts claiming work.

The supported version is available at runtime and can be verified against the
connected database:

```elixir
Absurd.SQL.supported_schema_version()
# => "0.5.0"

:ok = Absurd.SQL.verify_schema_version(MyApp.AbsurdDB)
```

Install the upstream schema separately using the
[`0.5.0` database instructions](https://github.com/earendil-works/absurd/blob/0.5.0/docs/database.md).
The SDK never installs or migrates the schema.

## Installation

Add the package from [Hex](https://hex.pm/packages/absurd) to `mix.exs`:

```elixir
def deps do
  [
    {:absurd, "~> 0.2.1"}
  ]
end
```

The SDK uses a Postgrex connection owned by your application. It does not
create a hidden connection pool.

## Guide

- [Architecture](ARCHITECTURE.md)
- [Quick start](#quick-start)
- [Clients and database ownership](#clients-and-database-ownership)
- [Queue management](#queue-management)
- [Task catalogs and worker pools](#task-catalogs-and-worker-pools)
- [Spawn options](#spawn-options)
- [Inspecting results](#inspecting-results)
- [Retry and cancellation](#retry-and-cancellation)
- [Durable context workflows](#durable-context-workflows)
- [Hooks](#hooks)
- [Telemetry](#telemetry)
- [Transactions](#transactions)
- [Errors and ambiguous writes](#errors-and-ambiguous-writes)
- [Maintenance and retention](#maintenance-and-retention)
- [Low-level SQL API](#low-level-sql-api)
- [Delivery semantics](#delivery-semantics)

## Quick start

### 1. Define a task

Task names are explicit because they are persisted independently of Elixir
module names:

```elixir
defmodule MyApp.SendWelcomeEmail do
  use Absurd.Task,
    name: "send-welcome-email",
    queue: "email",
    default_max_attempts: 5,
    default_cancellation: [max_duration: 120, max_delay: 30]

  @impl Absurd.Task
  def run(%{"user_id" => user_id}, context) do
    with {:ok, profile} <-
           Absurd.Context.step(context, "load-profile:v1", fn ->
             case MyApp.Accounts.fetch_profile(user_id) do
               {:ok, profile} ->
                 {:ok, %{"id" => profile.id, "email" => profile.email}}

               {:error, reason} ->
                 {:error, reason}
             end
           end),
         {:ok, receipt} <-
           Absurd.Context.step(context, "deliver-email:v1", fn ->
             case MyApp.Mailer.deliver_welcome(profile) do
               {:ok, receipt} -> {:ok, %{"id" => receipt.id}}
               {:error, reason} -> {:error, reason}
             end
           end) do
      {:ok, %{"receipt_id" => receipt["id"]}}
    end
  end
end
```

A task returns `{:ok, json_value}` or `{:error, reason}`. Parameters,
successful results, checkpoints, headers, and event payloads must be JSON
compatible and maps must have string keys.

Task cancellation durations are expressed in seconds. Client and context
timeouts are expressed in milliseconds.

### 2. Provision the queue

With your application's Postgrex connection running, create queues from a
deployment task or migration before enabling workers:

```elixir
client = Absurd.client(db: MyApp.AbsurdDB, queue: "email")

:ok = Absurd.Client.create_queue(client)
```

Queue creation is idempotent.

### 3. Supervise the database and workers

Place the Postgrex connection before each worker pool in your application's
supervision tree:

```elixir
children = [
  {Postgrex,
   name: MyApp.AbsurdDB,
   hostname: "localhost",
   database: "my_app",
   username: "postgres",
   password: "postgres"},
  {Absurd.WorkerPool,
   name: MyApp.EmailWorkers,
   db: MyApp.AbsurdDB,
   queue: "email",
   tasks: [MyApp.SendWelcomeEmail],
   concurrency: 10,
   batch_size: 10,
   claim_timeout: 120_000,
   poll_interval: 250,
   shutdown: 30_000}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Each pool consumes one queue. It validates its options, task catalog, and
schema before starting. Running tasks are isolated in temporary processes;
PostgreSQL remains the authority for recovery and retries.

### 4. Spawn and await work

```elixir
client = Absurd.client(db: MyApp.AbsurdDB, queue: "email")

{:ok, spawned} =
  Absurd.Client.spawn(
    client,
    MyApp.SendWelcomeEmail,
    %{"user_id" => "usr_123"},
    idempotency_key: "welcome:usr_123",
    headers: %{"trace_id" => "trace_456"}
  )

{:ok, %Absurd.TaskResult{state: :completed, result: result}} =
  Absurd.Client.await_task_result(client, spawned, timeout: 10_000)
```

`spawned` is an `Absurd.SpawnResult` containing the queue, binary task and run
UUIDs, attempt number, and whether this call created a new task.

## Clients and database ownership

`Absurd.Client` is an immutable value around a caller-owned Postgrex pool,
registered process, or checked-out connection:

```elixir
{:ok, client} =
  Absurd.Client.new(
    db: MyApp.AbsurdDB,
    queue: "default",
    default_max_attempts: 5,
    query_options: [timeout: 15_000]
  )

client = Absurd.Client.new!(db: MyApp.AbsurdDB, queue: "default")
client = Absurd.client(db: MyApp.AbsurdDB, queue: "default")
```

Use `new/1` when configuration errors should be returned and `new!/1` or
`Absurd.client/1` when invalid static configuration should raise.

## Queue management

Create an unpartitioned queue with defaults:

```elixir
:ok = Absurd.Client.create_queue(client, "default")
```

Or create a partitioned queue and set its maintenance policy at the same time:

```elixir
:ok =
  Absurd.Client.create_queue(client, "email",
    storage_mode: :partitioned,
    partition_lookahead: "35 days",
    partition_lookback: "2 days",
    cleanup_ttl: "30 days",
    cleanup_limit: 1_000,
    detach_mode: :empty,
    detach_min_age: "45 days"
  )
```

Policy intervals are PostgreSQL interval strings:

```elixir
:ok =
  Absurd.Client.set_queue_policy(client, "email",
    cleanup_ttl: "14 days",
    cleanup_limit: 500
  )

{:ok, %Absurd.QueuePolicy{} = policy} =
  Absurd.Client.get_queue_policy(client, "email")

{:ok, queues} = Absurd.Client.list_queues(client)
```

Dropping a queue deletes its upstream queue objects, so reserve it for explicit
administrative workflows:

```elixir
:ok = Absurd.Client.drop_queue(client, "retired-queue")
```

## Task catalogs and worker pools

For a small pool, pass task modules directly. Larger applications can define a
catalog module:

```elixir
defmodule MyApp.EmailTasks do
  @behaviour Absurd.TaskCatalog

  @impl Absurd.TaskCatalog
  def tasks do
    [MyApp.SendWelcomeEmail, MyApp.SendReceipt]
  end
end
```

Use it in the worker child:

```elixir
{Absurd.WorkerPool,
 name: MyApp.EmailWorkers,
 db: MyApp.AbsurdDB,
 queue: "email",
 tasks: MyApp.EmailTasks,
 concurrency: 20}
```

Catalog validation rejects missing callbacks, duplicate durable task names,
and task registrations for the wrong queue before the pool claims anything.
Use a separate named pool for each queue:

```elixir
children = [
  {Absurd.WorkerPool,
   name: MyApp.WorkflowWorkers,
   db: MyApp.AbsurdDB,
   queue: "workflows",
   tasks: [MyApp.RunWorkflow],
   concurrency: 8},
  {Absurd.WorkerPool,
   name: MyApp.EmailWorkers,
   db: MyApp.AbsurdDB,
   queue: "email",
   tasks: MyApp.EmailTasks,
   concurrency: 20}
]
```

## Spawn options

Registered modules use their durable name, queue, and task defaults. A single
spawn can override execution policy:

```elixir
{:ok, spawned} =
  Absurd.Client.spawn(
    client,
    MyApp.SendWelcomeEmail,
    %{"user_id" => "usr_123"},
    max_attempts: 8,
    retry_strategy: [
      kind: :exponential,
      base_seconds: 2,
      factor: 2,
      max_seconds: 300
    ],
    cancellation: [max_duration: 120, max_delay: 30],
    headers: %{
      "trace_id" => "trace_456",
      "requested_by" => "api"
    },
    idempotency_key: "welcome:usr_123"
  )
```

An idempotency key is scoped to its queue. Repeating a spawn with the same
queue and key returns the existing task with `created: false`:

```elixir
{:ok, first} =
  Absurd.Client.spawn(client, MyApp.SendWelcomeEmail, %{"user_id" => "usr_123"},
    idempotency_key: "welcome:usr_123"
  )

{:ok, second} =
  Absurd.Client.spawn(client, MyApp.SendWelcomeEmail, %{"user_id" => "usr_123"},
    idempotency_key: "welcome:usr_123"
  )

first.task_id == second.task_id
# => true

second.created
# => false
```

To interoperate with another Absurd SDK, spawn its persisted task name. Raw
names require an explicit queue:

```elixir
{:ok, task} =
  Absurd.Client.spawn(
    client,
    "task-implemented-by-another-sdk",
    %{"value" => 42},
    queue: "shared"
  )
```

## Inspecting results

Fetching is a single database snapshot; awaiting polls with bounded backoff:

```elixir
{:ok, snapshot_or_nil} = Absurd.Client.fetch_task_result(client, spawned)

{:ok, terminal_snapshot} =
  Absurd.Client.await_task_result(client, spawned, timeout: 30_000)
```

An `Absurd.TaskResult` has one of these states:

| State | Terminal? | Value |
|---|---:|---|
| `:pending` | no | waiting to be claimed |
| `:running` | no | currently leased to a worker |
| `:sleeping` | no | durably scheduled or waiting for an event |
| `:completed` | yes | `result` contains the JSON result |
| `:failed` | yes | `failure` contains bounded failure JSON |
| `:cancelled` | yes | no result |

Pattern-match the terminal outcome explicitly:

```elixir
case Absurd.Client.await_task_result(client, spawned, timeout: 30_000) do
  {:ok, %Absurd.TaskResult{state: :completed, result: result}} ->
    {:ok, result}

  {:ok, %Absurd.TaskResult{state: :failed, failure: failure}} ->
    {:error, {:task_failed, failure}}

  {:ok, %Absurd.TaskResult{state: :cancelled}} ->
    {:error, :task_cancelled}

  {:error, %Absurd.Error{kind: :timeout}} ->
    {:error, :caller_timed_out}

  {:error, %Absurd.Error{} = error} ->
    {:error, error}
end
```

A caller timeout does not cancel durable work. Fetch the same task later.
Fetching an unknown task returns `{:ok, nil}`; awaiting one returns an
`:unknown_task` error immediately.

## Retry and cancellation

Retry a failed task in place:

```elixir
{:ok, retry} =
  Absurd.Client.retry_task(client, failed_task, max_attempts: 10)

retry.created
# => false
```

Or create a new logical task from the failed task:

```elixir
{:ok, new_task} =
  Absurd.Client.retry_task(client, failed_task,
    max_attempts: 3,
    spawn_new: true
  )

new_task.created
# => true
```

Cancel active work by spawn result or binary task ID:

```elixir
:ok = Absurd.Client.cancel_task(client, spawned)
:ok = Absurd.Client.cancel_task(client, spawned.task_id, queue: "email")
```

Cancellation is cooperative. It changes durable state but cannot undo an
external effect already running in application code.

## Durable context workflows

`Absurd.Context` is passed to every task attempt. It exposes the current queue,
task and run IDs, attempt, worker ID, and immutable headers. Worker pools create
and close contexts automatically.

### Checkpoint a step

`step/3` runs its callback once per checkpoint occurrence and replays a
committed JSON value on later attempts:

```elixir
{:ok, customer} =
  Absurd.Context.step(context, "load-customer:v2", fn ->
    case MyApp.Customers.fetch(customer_id) do
      {:ok, customer} -> {:ok, %{"id" => customer.id, "email" => customer.email}}
      {:error, reason} -> {:error, reason}
    end
  end)
```

Repeating the same logical name in one execution allocates deterministic names
such as `load-customer:v2`, `load-customer:v2#2`, and so on. Keep task control
flow deterministic across attempts so the same occurrences mean the same work.
An explicit name that collides with an allocated occurrence is rejected.

### Make an external effect idempotent

The decomposed step API lets you derive a stable external idempotency key before
performing an effect:

```elixir
defp capture_payment(context, amount) do
  with {:ok, step} <- Absurd.Context.begin_step(context, "capture-payment:v1") do
    if step.done do
      {:ok, step.value}
    else
      idempotency_key = Absurd.Context.idempotency_key(context, step)

      with {:ok, charge} <-
             MyApp.Payments.capture(amount, idempotency_key: idempotency_key) do
        Absurd.Context.complete_step(
          context,
          step,
          %{"charge_id" => charge.id}
        )
      end
    end
  end
end
```

External effects can repeat if the effect succeeds but the process dies before
the checkpoint commits. Pass the derived key to systems that support
idempotency; checkpoints do not make arbitrary I/O exactly once.

### Extend a lease

Checkpoint writes extend the claim automatically. Long-running code without
checkpoints should heartbeat explicitly:

```elixir
Enum.reduce_while(batches, :ok, fn batch, :ok ->
  with :ok <- MyApp.Importer.process(batch),
       :ok <- Absurd.Context.heartbeat(context, 60_000) do
    {:cont, :ok}
  else
    {:error, reason} -> {:halt, {:error, reason}}
  end
end)
```

Heartbeat durations are milliseconds and round up to whole database seconds.

### Sleep without occupying a worker slot

```elixir
with :ok <- Absurd.Context.sleep_for(context, "wait-before-reminder:v1", 3_600_000) do
  {:ok, %{"ready" => true}}
end
```

Or choose an absolute UTC time:

```elixir
wake_at = DateTime.add(DateTime.utc_now(), 1, :day)

with :ok <- Absurd.Context.sleep_until(context, "wait-until-window:v1", wake_at) do
  {:ok, %{"window_open" => true}}
end
```

The wake time is checkpointed. A future sleep durably schedules the run,
unwinds the callback, and releases the worker slot. When claimed again, replay
continues after the sleep.

### Wait for and emit events

A task can suspend until an event arrives:

```elixir
event_name = "order:#{order_id}:approved"

with {:ok, approval} <-
       Absurd.Context.await_event(context, event_name, timeout: 86_400_000) do
  {:ok, %{"approved_by" => approval["user_id"]}}
end
```

Emit the event from a client:

```elixir
:ok =
  Absurd.Client.emit_event(
    client,
    "order:ord_123:approved",
    %{"user_id" => "usr_456"},
    queue: "workflows"
  )
```

Or emit on the current queue from another task:

```elixir
:ok = Absurd.Context.emit_event(context, event_name, %{"status" => "ready"})
```

Events are first-write-wins for a queue/name pair. Use names containing stable
business identity when each event should be distinct.

### Await a child task in another queue

Build a lightweight client around the context's existing database queryable,
spawn work in another queue, and checkpoint its terminal result:

```elixir
email_client = Absurd.client(db: context.db, queue: "email")

with {:ok, child} <-
       Absurd.Client.spawn(
         email_client,
         MyApp.SendReceipt,
         %{"order_id" => order_id},
         idempotency_key: "receipt:#{order_id}"
       ),
     {:ok, %Absurd.TaskResult{state: :completed, result: receipt}} <-
       Absurd.Context.await_task_result(context, child, timeout: 30_000) do
  {:ok, %{"receipt" => receipt}}
end
```

Child waits must cross queues. Waiting on the current queue is rejected before
polling because it can deadlock all worker slots. The terminal child snapshot
is checkpointed and replays even if the child queue is later removed.

### Read spawn headers

Headers are immutable execution metadata supplied at spawn time:

```elixir
trace_id = Map.get(context.headers, "trace_id")
attempt = context.attempt
```

Do not put secrets in headers; they are durable JSON.

## Hooks

Implement `Absurd.Hooks` to enrich spawn headers and wrap task execution. Both
callbacks are optional:

```elixir
defmodule MyApp.AbsurdHooks do
  @behaviour Absurd.Hooks

  require Logger

  @impl Absurd.Hooks
  def before_spawn(_task_name, _params, options) do
    headers = Keyword.get(options, :headers) || %{}
    trace_id = Integer.to_string(System.unique_integer([:positive]))

    {:ok, Keyword.put(options, :headers, Map.put(headers, "trace_id", trace_id))}
  end

  @impl Absurd.Hooks
  def wrap_task_execution(context, execute) do
    Logger.metadata(
      absurd_queue: context.queue,
      absurd_task: context.task_name,
      absurd_attempt: context.attempt
    )

    execute.()
  end
end
```

Configure the hook on clients, workers, or both:

```elixir
client =
  Absurd.client(
    db: MyApp.AbsurdDB,
    queue: "email",
    hooks: MyApp.AbsurdHooks
  )

{Absurd.WorkerPool,
 name: MyApp.EmailWorkers,
 db: MyApp.AbsurdDB,
 queue: "email",
 tasks: MyApp.EmailTasks,
 hooks: MyApp.AbsurdHooks}
```

A wrapper must invoke and return `execute.()` without swallowing throws or
exits; those include Absurd's private suspension and cancellation controls.

## Telemetry

SQL calls emit `[:absurd, :sql, operation, phase]`. Task executions emit
`[:absurd, :runner, :execute, phase]`. The phase is `:start`, `:stop`, or
`:exception`.

Attach a handler to the events your application uses:

```elixir
events = [
  [:absurd, :sql, :spawn_task, :stop],
  [:absurd, :sql, :spawn_task, :exception],
  [:absurd, :sql, :get_task_result, :stop],
  [:absurd, :runner, :execute, :stop],
  [:absurd, :runner, :execute, :exception]
]

:ok =
  :telemetry.attach_many(
    "my-app-absurd",
    events,
    &MyApp.AbsurdTelemetry.handle_event/4,
    nil
  )
```

```elixir
defmodule MyApp.AbsurdTelemetry do
  require Logger

  def handle_event(event, measurements, metadata, _config) do
    duration =
      measurements
      |> Map.get(:duration, 0)
      |> System.convert_time_unit(:native, :microsecond)

    Logger.debug(
      "Absurd event",
      event: event,
      duration_microseconds: duration,
      operation: metadata.operation,
      outcome: Map.get(metadata, :outcome)
    )
  end
end
```

Start measurements contain `system_time`; stop and exception measurements
contain monotonic `duration`. Metadata includes bounded operation and durable
identity fields, but never task parameters, results, headers, exception values,
or stacktraces. Telemetry reports what this SDK observed; it is not proof that
an ambiguous database write committed.

## Transactions

Every `Absurd.SQL` operation accepts a checked-out Postgrex connection. This
lets an application change its own tables and spawn a task atomically:

```elixir
{:ok, %Absurd.SpawnResult{} = spawned} =
  Postgrex.transaction(MyApp.AbsurdDB, fn connection ->
    Postgrex.query!(
      connection,
      "INSERT INTO orders (id, state) VALUES ($1, $2)",
      [order_id, "accepted"]
    )

    case Absurd.SQL.spawn_task(
           connection,
           "workflows",
           "fulfil-order",
           %{"order_id" => order_id},
           idempotency_key: "fulfil:#{order_id}"
         ) do
      {:ok, spawned} -> spawned
      {:error, error} -> Postgrex.rollback(connection, error)
    end
  end)
```

The low-level API does not perform an additional connection checkout.

## Errors and ambiguous writes

Public failures use `Absurd.Error` with a stable `kind`, readable message,
operation, optional SQLSTATE, and bounded diagnostics:

```elixir
case Absurd.Client.spawn(client, MyApp.SendWelcomeEmail, params,
       idempotency_key: idempotency_key
     ) do
  {:ok, spawned} ->
    {:ok, spawned}

  {:error, %Absurd.Error{kind: :ambiguous}} ->
    # The connection disappeared after a mutating query may have reached the
    # commit boundary. Repeating this idempotent spawn reconciles by queue/key.
    Absurd.Client.spawn(client, MyApp.SendWelcomeEmail, params,
      idempotency_key: idempotency_key
    )

  {:error, %Absurd.Error{} = error} ->
    {:error, error}
end
```

Important error kinds include `:validation`, `:configuration`, `:database`,
`:ambiguous`, `:protocol`, `:schema_incompatible`, `:cancelled`, `:failed_run`,
`:timeout`, and `:unknown_task`.

## Maintenance and retention

Policy-driven cleanup uses each queue's configured TTL and limit:

```elixir
{:ok, cleanup_results} = Absurd.SQL.cleanup_all_queues(MyApp.AbsurdDB)

Enum.each(cleanup_results, fn %Absurd.CleanupResult{} = result ->
  IO.inspect(result,
    label: "#{result.queue_name} cleanup"
  )
end)
```

Operators can also run bounded cleanup explicitly. TTL values here are
milliseconds:

```elixir
{:ok, tasks_deleted} =
  Absurd.SQL.cleanup_tasks(MyApp.AbsurdDB, "email",
    ttl: 30 * 24 * 60 * 60 * 1_000,
    limit: 1_000
  )

{:ok, events_deleted} =
  Absurd.SQL.cleanup_events(MyApp.AbsurdDB, "email",
    ttl: 7 * 24 * 60 * 60 * 1_000,
    limit: 1_000
  )
```

Schedule cleanup from your own supervision or operations system. The SDK does
not start a hidden maintenance process.

## Low-level SQL API

Most applications should use `Absurd.Client`, `Absurd.Task`,
`Absurd.Context`, and `Absurd.WorkerPool`. `Absurd.SQL` is public for custom
executors, operations tooling, compatibility tests, and checked-out
transactions. It exposes the upstream lifecycle closely:

- schema inspection and verification;
- queue creation, policy, listing, and deletion;
- spawn, result, retry, cancellation, and events;
- claims, completion, failure, scheduling, and lease extension;
- checkpoint reads and writes;
- event waits and bounded cleanup.

All SQL is fixed and parameterized. Callers provide the queryable and retain
ownership of connection, transaction, timeout, and retry policy.

## Delivery semantics

- Task execution is at least once across attempts and uncommitted checkpoints.
- A committed compatible checkpoint is replayed on a later attempt.
- External effects may repeat and should use stable idempotency keys.
- Unknown task names are deferred during rolling deployments; no fallback
  handler runs them.
- A worker pool stops claiming before it drains active runners.
- PostgreSQL—not process memory, mailboxes, logs, or telemetry—is the durable
  authority.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the fork and pull-request workflow,
local checks, integration database setup, and useful bug-report details.

Run the complete formatter, warnings-as-errors compiler, strict Credo, test,
doctest, documentation coverage, and ExDoc gate with:

```console
mix check
```

Set `ABSURD_INTEGRATION_DATABASE_URL` to include the compatibility and worker
failure suites against a PostgreSQL database containing the supported Absurd
`0.5.0` schema.

## License

Apache License 2.0. See [LICENSE](LICENSE).
