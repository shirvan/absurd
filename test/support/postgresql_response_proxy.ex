defmodule Absurd.TestPostgreSQLResponseProxy do
  @moduledoc false

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @spec port(pid()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @spec arm(pid(), atom(), pid()) :: :ok
  def arm(proxy, operation, observer) do
    GenServer.call(proxy, {:arm, operation, observer})
  end

  @spec arm_on_query([atom()], map(), map(), {pid(), pid()}) :: :ok
  def arm_on_query(_event, _measurements, metadata, {proxy, observer}) do
    arm(proxy, metadata.operation, observer)
  end

  @impl GenServer
  def init(options) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    proxy = self()
    spawn_link(fn -> accept(listener, proxy, options) end)
    {:ok, %{listener: listener, port: port, armed: nil}}
  end

  @impl GenServer
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def handle_call({:arm, operation, observer}, _from, state) do
    {:reply, :ok, %{state | armed: {operation, observer}}}
  end

  def handle_call(:take_arm, _from, state), do: {:reply, state.armed, %{state | armed: nil}}

  @impl GenServer
  def terminate(_reason, state), do: :gen_tcp.close(state.listener)

  defp accept(listener, proxy, options) do
    case :gen_tcp.accept(listener) do
      {:ok, client} ->
        handler =
          spawn_link(fn ->
            receive do
              :socket_ready -> connect(client, proxy, options)
            end
          end)

        :ok = :gen_tcp.controlling_process(client, handler)
        send(handler, :socket_ready)
        accept(listener, proxy, options)

      {:error, :closed} ->
        :ok
    end
  end

  defp connect(client, proxy, options) do
    host = options |> Keyword.fetch!(:hostname) |> String.to_charlist()
    port = Keyword.get(options, :port, 5432)
    {:ok, backend} = :gen_tcp.connect(host, port, [:binary, active: false], 5_000)

    sender =
      spawn_link(fn ->
        receive do
          :socket_ready -> relay_client(client, backend)
        end
      end)

    :ok = :gen_tcp.controlling_process(client, sender)
    send(sender, :socket_ready)

    try do
      relay_server(client, backend, proxy, nil)
    after
      :gen_tcp.close(client)
      :gen_tcp.close(backend)
    end
  end

  defp relay_client(client, backend) do
    with {:ok, bytes} <- :gen_tcp.recv(client, 0),
         :ok <- :gen_tcp.send(backend, bytes) do
      relay_client(client, backend)
    else
      {:error, _reason} -> :gen_tcp.close(backend)
    end
  end

  defp relay_server(client, backend, proxy, dropping) do
    with {:ok, <<type, length::32>> = header} <- :gen_tcp.recv(backend, 5),
         {:ok, body} <- read_body(backend, length - 4) do
      dropping =
        if type == ?D and is_nil(dropping),
          do: GenServer.call(proxy, :take_arm),
          else: dropping

      relay_response(type, [header, body], client, backend, proxy, dropping)
    else
      {:error, _reason} -> :ok
    end
  end

  defp relay_response(?Z, _bytes, _client, _backend, _proxy, {operation, observer}) do
    # Hold the DataRow and everything after it, then close only after PostgreSQL
    # sends ReadyForQuery: the implicit transaction committed, but Postgrex never
    # received its result. Parse/describe responses are forwarded before arming.
    send(observer, {:response_dropped, operation})
    :ok
  end

  defp relay_response(_type, bytes, client, backend, proxy, nil) do
    case :gen_tcp.send(client, bytes) do
      :ok -> relay_server(client, backend, proxy, nil)
      {:error, _reason} -> :ok
    end
  end

  defp relay_response(_type, _bytes, client, backend, proxy, dropping) do
    relay_server(client, backend, proxy, dropping)
  end

  defp read_body(_backend, 0), do: {:ok, <<>>}
  defp read_body(backend, length), do: :gen_tcp.recv(backend, length)
end
