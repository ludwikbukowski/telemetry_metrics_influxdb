# telemetry_influxdb
InfluxDB reporter for [Telemetry](https://github.com/beam-telemetry/telemetry)

`Telemetry` reporter for InfluxDB compatible events.

  To use it, start the reporter with the `start_link/1` function, providing it a list of
  `Telemetry` event names:

  ```elixir
      TelemetryInfluxDB.start_link(
        events: [
          %{name: [:memory, :usage], metadata_tag_keys: [:host, :ip_address]},
          %{name: [:http, :request]},
        ]
      )
  ```

  or put it under a supervisor:

  ```elixir
  children = [
    {TelemetryInfluxDB, [
      events: [
        %{name: [:memory, :usage], metadata_tag_keys: [:host, :ip_address]},
        %{name: [:http, :request]}
    ]}
  ]

  Supervisor.start_link(children, ...)
  ```

  By default the reporter sends events through UDP to localhost:8089.

  Note that the reporter doesn't aggregate events in-process by default - it sends updates to InfluxDB
  whenever a relevant Telemetry event is emitted. See the `:batch_size` option below to configure this behavior.

## Run test

Running the tests currently requires [jq](https://stedolan.github.io/jq/). Please make sure you have it installed before running the tests.

```
$ make test
```

It should setup the latest InfluxDB in docker for both v1 and v2 and runs all the tests against them.

## Configuration

Possible options for the reporter:

### Options for Any InfluxDB Version
 - `:version` - :v1 or :v2. The version of InfluxDB to use; defaults to :v1 if not provided
 - `:reporter_name` - unique name for the reporter. The purpose is to distinguish between different reporters running in the system.
    One can run separate independent InfluxDB reporters, with different configurations and goals.
 - `:protocol` - :udp or :http. Which protocol to use for connecting to InfluxDB. Default option is :udp. InfluxDB v2 only supports :http for now.
 - `:host` - host, where InfluxDB is running.
 - `:port` - port, where InfluxDB is running.
 - `:events` - list of `Telemetry` events' names that we want to send to InfluxDB.
    Each event should be specified by the map with the field `name`, e.g. `%{name: [:sample, :event, :name]}`.
    Event names should be compatible with `Telemetry` events' format.
    It is also possible to specify an optional list of metadata keys that will be included in the event body and sent to InfluxDB as tags.
    The list of metadata keys should be specified in the event data with the field `metadata_tag_keys`, e.g. `%{name: [:sample, :event, :name], metadata_tag_keys: [:sample_meta, sample_meta2]}`
 - `:batch_size` - the maximum number of events to send to InfluxDB at one time. (default 1)
 - `:tags` - list of global static tags, that will be attached to each reported event. The format is a map,
    where the key and the value are tag's name and value, respectively.
    Both the tag's name and the value could be atoms or binaries.
 - `:worker_pool_size` - specify the number of workers to use to send events to InfluxDB via HTTP. Only used when `:protocol` is set to `:http`. (default 3)
### V1 Only Options
 - `:db` - name of the location where time series data is stored in InfluxDB v1
 - `:username` - username of InfluxDB's user that has writes privileges. Only required in v1.
 - `:password` - password for the user. Only required for v1.
### V2 Only Options
 - `:bucket` - name of the location where time series data is stored in InfluxDB v2
 - `:org` -  workspace in InfluxDB v2 where a bucket belongs
 - `:token` - InfluxDB v2 authentication token used for authenticating requests. Must have write privileges to the bucket and org specified.

## Notes

For the HTTP protocol, [worker_pool](https://github.com/inaka/worker_pool) is used for sending requests asynchronously.
Therefore the HTTP requests are sent in the context of the separate workers' pool, which does not block the client's application
(it is not sent in the critical path of the client's process).

On the other hand, UDP packets are sent in the context of the processes that execute the events.
However, the lightweight nature of UDP should not cause any bottlenecks in such a solution.

Events are sent straightaway without any batching techniques by default. If the `:batch_size` option is set to a value higher than 1, all events received since the previous batch will be sent in the next batch (up to a maximum of `:batch_size` events).

Once the reporter is started, it is attached to specified `Telemetry` events.
The events are detached when the reporter is shutdown.

## Copyright and License

TelemetryInfluxDB is copyright (c) 2019 Ludwik Bukowski.

TelemetryInfluxDB source code is released under MIT license.

See [LICENSE](LICENSE) for more information.
