defmodule Absurd do
  @moduledoc """
  Unofficial Elixir client and OTP worker application for Absurd.

  SDK `0.1.x` supports the upstream Absurd `0.5.0` schema. The SDK and schema
  versions are independent; `Absurd.SQL.verify_schema_version/2` checks the
  database explicitly, and every `Absurd.WorkerPool` checks it at startup.

  PostgreSQL and the upstream Absurd schema own durable task state. This
  package owns the Elixir API and the OTP processes which claim and execute
  work.

  Use `client/1` to build a process-free `Absurd.Client`. Add
  `Absurd.WorkerPool` after the caller-owned Postgrex process in the host
  application's supervision tree when this node should execute tasks.

  ## Examples

      iex> client = Absurd.client(db: self())
      iex> client.queue
      "default"

      iex> Absurd.SQL.supported_schema_version()
      "0.5.0"
  """

  alias Absurd.Client

  @doc """
  Creates a process-free client value from explicit options.

  See `Absurd.Client.new!/1` for all supported options and validation rules.
  """
  @spec client(keyword()) :: Client.t()
  def client(options), do: Client.new!(options)
end
