defmodule Absurd.PostgreSQLCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Absurd.Client
  alias Absurd.SQL

  using do
    quote do
      use ExUnit.Case, async: false

      import Absurd.PostgreSQLCase,
        only: [clear_fake_now: 1, set_fake_now: 2, unique_queue: 1]

      @moduletag :postgresql
    end
  end

  setup_all do
    database_url = System.fetch_env!("ABSURD_INTEGRATION_DATABASE_URL")
    db = start_supervised!({Postgrex, connection_options(database_url)})

    assert :ok = SQL.verify_schema_version(db)

    {:ok, db: db}
  end

  setup %{db: db} do
    queue = unique_queue("case")
    client = Client.new!(db: db, queue: queue)

    assert :ok = Client.create_queue(client)
    on_exit(fn -> assert :ok = SQL.drop_queue(db, queue) end)

    {:ok, client: client, db: db, queue: queue}
  end

  @spec unique_queue(String.t()) :: String.t()
  def unique_queue(label) do
    suffix = System.unique_integer([:positive, :monotonic])
    "elixir_#{label}_#{suffix}"
  end

  @spec set_fake_now(GenServer.server(), DateTime.t()) :: Postgrex.Result.t()
  def set_fake_now(db, datetime) do
    Postgrex.query!(
      db,
      "SELECT set_config('absurd.fake_now', $1, false)",
      [DateTime.to_iso8601(datetime)]
    )
  end

  @spec clear_fake_now(GenServer.server()) :: Postgrex.Result.t()
  def clear_fake_now(db) do
    Postgrex.query!(db, "SELECT set_config('absurd.fake_now', '', false)", [])
  end

  defp connection_options(database_url) do
    case URI.parse(database_url) do
      %URI{
        scheme: scheme,
        host: hostname,
        port: port,
        path: "/" <> database,
        userinfo: userinfo
      }
      when scheme in ["postgres", "postgresql"] and is_binary(hostname) and
             is_binary(userinfo) and database != "" ->
        {username, password} = split_userinfo(userinfo)

        [
          hostname: hostname,
          port: port || 5432,
          database: URI.decode(database),
          username: URI.decode(username),
          password: URI.decode(password)
        ]

      _other ->
        raise ArgumentError,
              "ABSURD_INTEGRATION_DATABASE_URL must be a PostgreSQL URL with credentials"
    end
  end

  defp split_userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] when username != "" -> {username, password}
      _other -> raise ArgumentError, "PostgreSQL integration URL must include user and password"
    end
  end
end
