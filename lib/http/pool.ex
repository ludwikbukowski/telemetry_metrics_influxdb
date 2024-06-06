defmodule TelemetryInfluxDB.HTTP.Pool do
  @moduledoc false
  require Logger
  alias TelemetryInfluxDB, as: InfluxDB

  @spec child_spec(InfluxDB.config()) :: Supervisor.child_spec()
  def child_spec(config) do
    config = %{config | port: :erlang.integer_to_binary(config.port)}

    delete_old_pool_ets(config.reporter_name)
    insert_pool_ets(config.reporter_name, pool_name(config.reporter_name))

    %{
      id: pool_name(config.reporter_name),
      start:
        {:wpool, :start_pool,
         [pool_name(config.reporter_name), [{:workers, config.worker_pool_size}]]}
    }
  end

  defp pool_name(prefix) do
    :erlang.binary_to_atom(prefix <> "_WorkerPool", :utf8)
  end

  def get_name(prefix) do
    case :ets.lookup(table_name(prefix), "pool") do
      [{"pool", pool_name}] -> pool_name
      _ -> :no_pool
    end
  end

  defp insert_pool_ets(prefix, pool_name) do
    :ets.insert(table_name(prefix), {"pool", pool_name})
  end

  defp delete_old_pool_ets(prefix) do
    :ets.delete(table_name(prefix), "pool")
  end

  defp table_name(prefix) do
    :erlang.binary_to_atom(prefix <> "_influx_reporter", :utf8)
  end
end
