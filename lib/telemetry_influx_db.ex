defmodule TelemetryInfluxDB do
  alias TelemetryInfluxDB.BatchReporter
  alias TelemetryInfluxDB.BatchHandler
  alias TelemetryInfluxDB.EventHandler
  alias TelemetryInfluxDB.HTTP
  alias TelemetryInfluxDB.UDP
  require Logger
  use Supervisor

  @moduledoc """
  `Telemetry` reporter for InfluxDB compatible events.

  To use it, start the reporter with the `start_link/1` function, providing it a list of
  `Telemetry` event names:
  ```elixir
     TelemetryMetricsInfluxDB.start_link(
       events: [
         %{name: [:memory, :usage], metadata_tag_keys: [:host, :ip_address]},
         %{name: [:http, :request]},
       ]
     )
  ```
  > Note that in the real project the reporter should be started under a supervisor, e.g. the main
  > supervisor of your application.

  By default, the reporter sends events through UDP to localhost:8089.

  Note that the reporter doesn't aggregate events in-process - it sends updates to InfluxDB
  whenever a relevant Telemetry event is emitted.

  #### Configuration

  Possible options for the reporter:

  Options for any InfluxDB version:
     * `:version` - :v1 or :v2. The version of InfluxDB to use; defaults to :v1 if not provided
     * `:reporter_name` - unique name for the reporter. The purpose is to distinguish between different reporters running in the system.
     * `:batch_size` - maximum number of events to send to InfluxDB in a single batch (default 1: no batching)
     One can run separate independent InfluxDB reporters, with different configurations and goals.
     * `:protocol` - :udp or :http. Which protocol to use for connecting to InfluxDB. Default option is :udp. InfluxDB v2 only supports :http for now.
     * `:host` - host, where InfluxDB is running.
     * `:port` - port, where InfluxDB is running.
     * `:events` - list of `Telemetry` events' names that we want to send to InfluxDB.
     Each event should be specified by the map with the field `name`, e.g. %{name: [:sample, :event, :name]}.
     Event names should be compatible with `Telemetry` events' format.
     It is also possible to specify an optional list of metadata keys that will be included in the event body and sent to InfluxDB as tags.
     The list of metadata keys should be specified in the event data with the field `metadata_tag_keys`, e.g. %{name: [:sample, :event, :name], metadata_tag_keys: [:sample_meta, sample_meta2]}
     * `:tags` - list of global static tags, that will be attached to each reported event. The format is a map,
     where the key and the value are tag's name and value, respectively.
     Both the tag's name and the value could be atoms or binaries.

  V1 Only Options
     * `:db` - name of the location where time series data is stored in InfluxDB v1
     * `:username` - username of InfluxDB's user that has writes privileges. Only required in v1.
     * `:password` - password for the user. Only required in v1.

  V2 Only Options
     * `:bucket` - name of the location where time series data is stored in InfluxDB v2
     * `:org` -  workspace in InfluxDB v2 where a bucket belongs
     * `:token` - InfluxDB v2 authentication token used for authenticating requests. Must have write privileges to the bucket and org specified.

  #### Notes

  For the HTTP protocol, [worker_pool](https://github.com/inaka/worker_pool) is used for sending requests asynchronously.
  Therefore the HTTP requests are sent in the context of the separate workers' pool, which does not block the client's application
  (it is not sent in the critical path of the client's process).
  The events are sent straightaway without any batching techniques.
  On the other hand, UDP packets are sent in the context of the processes that execute the events.
  However, the lightweight nature of UDP should not cause any bottlenecks in such a solution.

  Once the reporter is started, it is attached to specified `Telemetry` events.
  The events are detached when the reporter is shutdown.

  """

  @default_port 8089

  @type option ::
          {:port, :inet.port_number()}
          | {:host, String.t()}
          | {:protocol, atom()}
          | {:reporter_name, binary()}
          | {:batch_size, non_neg_integer()}
          | {:version, atom()}
          | {:db, String.t()}
          | {:org, String.t()}
          | {:bucket, String.t()}
          | {:username, String.t()}
          | {:password, String.t()}
          | {:token, String.t()}
          | {:events, [event]}
          | {:tags, tags}
          | {:worker_pool_size, non_neg_integer()}

  @type options :: [option]
  @type event :: %{required(:name) => :telemetry.event_name()}
  @type tags :: map()
  @type event_spec() :: map()
  @type event_name() :: [atom()]
  @type event_measurements :: map()
  @type event_metadata :: map()
  @type config :: map()
  @type handler_id() :: term()

  @spec start_link(options) :: GenServer.on_start()
  def start_link(options) do
    Supervisor.start_link(__MODULE__, options)
  end

  @impl true
  def init(options) do
    config =
      options
      |> Enum.into(%{})
      |> Map.put_new(:reporter_name, "default")
      |> Map.put_new(:batch_size, 1)
      |> Map.put_new(:protocol, :udp)
      |> Map.put_new(:host, "localhost")
      |> Map.put_new(:port, @default_port)
      |> Map.put_new(:tags, %{})
      |> Map.put_new(:worker_pool_size, 3)
      |> Map.put_new(:version, :v1)
      |> validate_required!([:events])
      |> validate_event_fields!()
      |> validate_protocol!()
      |> validate_version_params!()
      |> add_publisher()

    create_ets(config.reporter_name)
    specs = child_specs(config.protocol, config)
    Supervisor.init(specs, strategy: :one_for_all)
  end

  defp add_publisher(%{protocol: :http} = config) do
    Map.put(config, :publisher, HTTP.Publisher)
  end

  defp add_publisher(%{protocol: :udp} = config) do
    Map.put(config, :publisher, UDP.Publisher)
  end

  defp create_ets(prefix) do
    try do
      :ets.new(table_name(prefix), [:set, :public, :named_table])
    rescue
      _ ->
        :ok
    end
  end

  defp table_name(prefix) do
    :erlang.binary_to_atom(prefix <> "_influx_reporter", :utf8)
  end

  def stop(pid) do
    Supervisor.stop(pid)
  end

  defp child_specs(protocol, config) do
    publisher_child_specs(protocol, config) ++ common_child_specs(config)
  end

  defp publisher_child_specs(:http, config), do: [HTTP.Pool.child_spec(config)]

  defp publisher_child_specs(:udp, config),
    do: [%{id: UDP.Connector, start: {UDP.Connector, :start_link, [config]}}]

  defp common_child_specs(config) do
    [
      %{id: EventHandler, start: {EventHandler, :start_link, [config]}},
      %{id: BatchReporter, start: {BatchReporter, :start_link, [batch_reporter_options(config)]}}
    ]
  end

  def batch_reporter_options(config) do
    [
      name: BatchReporter.get_name(config),
      batch_size: config.batch_size,
      report_fn: &BatchHandler.handle_batch/1
    ]
  end

  defp validate_protocol!(%{protocol: :udp} = opts), do: opts
  defp validate_protocol!(%{protocol: :http} = opts), do: opts

  defp validate_protocol!(_) do
    raise(ArgumentError, "protocol has to be :udp or :http")
  end

  defp validate_version_params!(%{version: :v2} = opts), do: validate_v2_params!(opts)
  defp validate_version_params!(%{version: :v1} = opts), do: validate_v1_params!(opts)

  defp validate_version_params!(_opts) do
    raise(
      ArgumentError,
      "version must be :v1 or :v2"
    )
  end

  defp validate_v2_params!(%{protocol: :http, org: _org, bucket: _bucket, token: _token} = opts),
    do: opts

  defp validate_v2_params!(%{protocol: :udp}) do
    raise(
      ArgumentError,
      "the udp protocol is not currently supported for InfluxDB v2; please use http instead"
    )
  end

  defp validate_v2_params!(_) do
    raise(ArgumentError, "for InfluxDB v2 you need to specify :bucket, :org, and :token fields")
  end

  defp validate_v1_params!(%{protocol: :udp} = opts), do: opts
  defp validate_v1_params!(%{protocol: :http, db: _db} = opts), do: opts

  defp validate_v1_params!(_),
    do: raise(ArgumentError, "for http protocol in v1 you need to specify :db field")

  defp validate_event_fields!(%{events: []}) do
    raise(ArgumentError, "you need to attach to at least one event")
  end

  defp validate_event_fields!(%{events: events} = opts) when is_list(events) do
    Enum.map(events, &validate_required!(&1, :name))
    opts
  end

  defp validate_event_fields!(%{events: _}) do
    raise(ArgumentError, ":events needs to be list of events")
  end

  defp validate_required!(opts, fields) when is_list(fields) do
    Enum.map(fields, &validate_required!(opts, &1))
    opts
  end

  defp validate_required!(opts, field) do
    case Map.has_key?(opts, field) do
      true ->
        opts

      false ->
        raise(ArgumentError, "#{inspect(field)} field needs to be specified")
    end
  end
end
