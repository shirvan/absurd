import Config

# Worker pools are intentionally opt-in. A client-only application leaves this
# list empty; configured pools become children of Absurd.Supervisor.
config :absurd, worker_pools: []
