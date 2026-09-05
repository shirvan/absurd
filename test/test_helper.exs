postgresql_exclusions =
  if System.get_env("ABSURD_INTEGRATION_DATABASE_URL") do
    []
  else
    [:postgresql]
  end

ExUnit.start(exclude: postgresql_exclusions)

Code.require_file("support/postgresql_case.ex", __DIR__)
Code.require_file("support/telemetry_handler.ex", __DIR__)
Code.require_file("support/postgresql_response_proxy.ex", __DIR__)
Code.require_file("support/worker_tasks.ex", __DIR__)
