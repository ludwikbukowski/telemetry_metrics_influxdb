defmodule TelemetryInfluxDBTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Eventually

  alias TelemetryInfluxDB.Test.FluxParser
  alias TelemetryInfluxDB.Test.InfluxSimpleClient
  alias TelemetryInfluxDB.UDP

  @v1_config %{
    version: :v1,
    db: "myinflux",
    username: "myuser",
    password: "mysecretpassword",
    host: "localhost",
    protocol: :udp,
    port: 8087
  }

  @v2_config %{
    bucket: "myinflux",
    host: "localhost",
    org: "myorg",
    port: 9999,
    protocol: :http,
    token: "mysecrettoken",
    version: :v2
  }

  describe "Invalid reporter configuration - " do
    test "error log message is displayed for invalid influxdb credentials" do
      # given
      event = given_event_spec([:request, :failed])

      config =
        make_config(%{version: :v1, protocol: :http}, %{
          events: [event],
          username: "badguy",
          password: "wrongpass"
        })

      pid = start_reporter(config)
      testpid = self()

      :meck.new(TelemetryInfluxDB.HTTP.Publisher, [:unstick, :passthrough])

      :meck.expect(TelemetryInfluxDB.HTTP.Publisher, :send_event, fn q, b, h ->
        res = :meck.passthrough([q, b, h])
        send(testpid, :event_sent)
        res
      end)

      log =
        capture_log(fn ->
          # when
          :telemetry.execute([:request, :failed], %{"user" => "invalid", "password" => "invalid"})
          assert_receive :event_sent, 500
        end)

      ## then
      assert log =~ "Failed to push data to InfluxDB. Invalid credentials"
      stop_reporter(pid)
      :meck.unload(TelemetryInfluxDB.HTTP.Publisher)
    end

    test "error log message is displayed for invalid influxdb database" do
      # given
      event = given_event_spec([:users, :count])

      config =
        make_config(
          %{
            version: :v1,
            protocol: :http
          },
          %{events: [event], db: "yy_postgres"}
        )

      pid = start_reporter(config)
      testpid = self()
      :meck.new(TelemetryInfluxDB.HTTP.Publisher, [:unstick, :passthrough])

      :meck.expect(TelemetryInfluxDB.HTTP.Publisher, :send_event, fn q, b, h ->
        res = :meck.passthrough([q, b, h])
        send(testpid, :event_sent)
        res
      end)

      log =
        capture_log(fn ->
          # when
          :telemetry.execute([:users, :count], %{"value" => "30"})
          assert_receive :event_sent, 200
        end)

      # then
      assert log =~ "Failed to push data to InfluxDB. Invalid credentials"
      stop_reporter(pid)
      :meck.unload(TelemetryInfluxDB.HTTP.Publisher)
    end

    test "error log message is displayed for missing db for HTTP" do
      assert_raise(
        ArgumentError,
        "for http protocol in v1 you need to specify :db field",
        fn ->
          @v1_config
          |> Map.delete(:db)
          |> Map.put(:protocol, :http)
          |> Map.put(:events, [given_event_spec([:missing, :db])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing bucket in v2 config" do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @v2_config
          |> Map.delete(:bucket)
          |> Map.put(:events, [given_event_spec([:missing, :bucket])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing org in v2 config" do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @v2_config
          |> Map.delete(:org)
          |> Map.put(:events, [given_event_spec([:missing, :org])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for invalid version" do
      assert_raise(
        ArgumentError,
        "version must be :v1 or :v2",
        fn ->
          @v1_config
          |> Map.put(:version, :bad_version)
          |> Map.put(:events, [given_event_spec([:invalid, :version])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed for missing token in v2 config" do
      assert_raise(
        ArgumentError,
        "for InfluxDB v2 you need to specify :bucket, :org, and :token fields",
        fn ->
          @v2_config
          |> Map.delete(:token)
          |> Map.put(:events, [given_event_spec([:missing, :token])])
          |> start_reporter()
        end
      )
    end

    test "error message is displayed when specifying udp protocol with v2 config" do
      assert_raise(
        ArgumentError,
        "the udp protocol is not currently supported for InfluxDB v2; please use http instead",
        fn ->
          @v2_config
          |> Map.put(:protocol, :udp)
          |> Map.put(:events, [given_event_spec([:v2, :udp])])
          |> start_reporter()
        end
      )
    end
  end

  describe "Events reported - " do
    for {version, protocol} <- [{:v1, :http}, {:v1, :udp}, {:v2, :http}] do
      @tag protocol: protocol
      @tag version: version
      test "event is reported when specified by its name for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:requests, :failed])
        config = make_config(context, %{events: [event]})
        pid = start_reporter(config)

        ## when
        :telemetry.execute([:requests, :failed], %{"reason" => "timeout", "retries" => 3})

        ## then
        assert_reported(context, "requests.failed", %{"reason" => "timeout", "retries" => 3})

        ## cleanup
        clear_series(context, "requests.failed")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "event is reported with correct data types for #{version} #{protocol} API", context do
        ## given
        event = given_event_spec([:calls, :failed])
        config = make_config(context, %{events: [event]})
        pid = start_reporter(config)

        ## when
        :telemetry.execute([:calls, :failed], %{
          "int" => 4,
          "string_int" => "3",
          "float" => 0.34,
          "string" => "random",
          "boolean" => true
        })

        ## then
        assert_reported(context, "calls.failed", %{
          "int" => 4,
          "string_int" => "3",
          "float" => 0.34,
          "string" => "random",
          "boolean" => true
        })

        ## cleanup
        clear_series(context, "calls.failed")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "only specified events are reported for #{version} #{protocol} API", context do
        ## given
        event1 = given_event_spec([:event, :one])
        event2 = given_event_spec([:event, :two])
        event3 = given_event_spec([:event, :three])
        config = make_config(context, %{events: [event1, event2, event3]})
        pid = start_reporter(config)
        ## when
        :telemetry.execute([:event, :one], %{"value" => 1})
        assert_reported(context, "event.one", %{"value" => 1})

        :telemetry.execute([:event, :two], %{"value" => 2})
        assert_reported(context, "event.two", %{"value" => 2})

        :telemetry.execute([:event, :other], %{"value" => "?"})

        ## then
        refute_reported(context, "event.other")

        ## cleanup
        clear_series(context, "event.one")
        clear_series(context, "event.two")
        clear_series(context, "event.other")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with global pre-defined tags for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:memory, :leak])

        config =
          make_config(context, %{
            events: [event],
            tags: %{region: :eu_central, time_zone: :cest}
          })

        pid = start_reporter(config)

        ## when
        :telemetry.execute([:memory, :leak], %{"memory_leaked" => 100})

        ## then
        assert_reported(context, "memory.leak", %{"memory_leaked" => 100}, %{
          "region" => "eu_central",
          "time_zone" => "cest"
        })

        ## cleanup
        clear_series(context, "memory.leak")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with event-specific tags for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:system, :crash])
        config = make_config(context, %{events: [event], tags: %{}})
        pid = start_reporter(config)

        ## when
        :telemetry.execute([:system, :crash], %{"node_id" => "a3"}, %{tags: %{priority: :high}})

        ## then
        assert_reported(context, "system.crash", %{"node_id" => "a3"}, %{
          "priority" => "high"
        })

        ## cleanup
        clear_series(context, "system.crash")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported with metadata tags specified for #{version} #{protocol} API",
           context do
        ## given
        event = given_event_spec([:database, :repo], [:hostname])
        config = make_config(context, %{events: [event]})
        pid = start_reporter(config)

        ## when
        :telemetry.execute([:database, :repo], %{"query_time" => 0.01}, %{hostname: "host-01"})

        ## then
        assert_reported(context, "database.repo", %{"query_time" => 0.01}, %{
          "hostname" => "host-01"
        })

        ## cleanup
        clear_series(context, "database.repo")
        stop_reporter(pid)
      end

      @tag protocol: protocol
      @tag version: version
      test "events are reported with special characters for #{version} #{protocol} API",
           context do
        ## given
        event1 = given_event_spec([:event, :special1])
        event2 = given_event_spec([:event, :special2])
        config = make_config(context, %{events: [event1, event2], tags: %{}})
        pid = start_reporter(config)

        ## when
        :telemetry.execute([:event, :special1], %{"equal_sign" => "a=b"}, %{
          tags: %{priority: "hig\"h"}
        })

        :telemetry.execute([:event, :special2], %{"comma_space" => "a,b c"}, %{tags: %{}})

        ## then
        assert_reported(context, "event.special1", %{"equal_sign" => "a\\\=b"}, %{
          "priority" => "hig\\\"h"
        })

        assert_reported(context, "event.special2", %{"comma_space" => "a\\,b\\ c"}, %{})

        ## cleanup
        clear_series(context, "event.special1")
        clear_series(context, "event.special2")
        stop_reporter(pid)
      end

      @tag version: version
      @tag protocol: protocol
      test "events are detached after stopping reporter for #{version} #{protocol} API",
           context do
        ## given
        event_old = given_event_spec([:old, :event])
        event_new = given_event_spec([:new, :event])
        config = make_config(context, %{events: [event_old, event_new]})
        pid = start_reporter(config)

        :telemetry.execute([:old, :event], %{"value" => 1})
        assert_reported(context, "old.event", %{"value" => 1})

        ## when
        TelemetryInfluxDB.stop(pid)
        :telemetry.execute([:new, :event], %{"value" => 2})

        ## then
        refute_reported(context, "new.event")

        ## cleanup
        clear_series(context, "old.event")
      end

      @tag :capture_log
      @tag version: version
      @tag protocol: protocol
      test "events are not reported when reporter receives an exit signal for #{version} #{
             protocol
           } API",
           context do
        ## given
        event_first = given_event_spec([:first, :event])
        event_second = given_event_spec([:second, :event])
        config = make_config(context, %{events: [event_first, event_second]})
        pid = start_reporter(config)

        Process.unlink(pid)
        {:links, child_pids} = :erlang.process_info(pid, :links)

        # Make sure that event handlers are detached even if non-parent process sends an exit signal.

        spawn(fn -> Process.exit(pid, :kill) end)
        wait_processes_to_die(child_pids ++ [pid])

        assert :telemetry.list_handlers([:first, :event]) == []
        assert :telemetry.list_handlers([:second, :event]) == []

        :telemetry.execute([:first, :event], %{})
        :telemetry.execute([:second, :event], %{})

        refute_reported(context, "first.event")
        refute_reported(context, "second.event")
      end

      @tag version: version
      @tag protocol: protocol
      test "events are reported from two independent reporters for #{version} #{protocol} API",
           context do
        ## given
        event1 = given_event_spec([:servers1, :down])
        event2 = given_event_spec([:servers2, :down])

        config =
          make_config(context, %{
            events: [event1],
            tags: %{region: :eu_central, time_zone: :cest},
            reporter_name: "eu"
          })

        pid1 = start_reporter(config)

        config =
          make_config(context, %{
            events: [event2],
            tags: %{region: :asia, time_zone: :other},
            reporter_name: "asia"
          })

        pid2 = start_reporter(config)

        ## when
        :telemetry.execute([:servers1, :down], %{"panic?" => "yes"})
        :telemetry.execute([:servers2, :down], %{"panic?" => "yes"})

        ## then
        assert_reported(context, "servers1.down", %{"panic?" => "yes"}, %{
          "region" => "eu_central",
          "time_zone" => "cest"
        })

        assert_reported(context, "servers2.down", %{"panic?" => "yes"}, %{
          "region" => "asia",
          "time_zone" => "other"
        })

        ## cleanup
        clear_series(context, "servers1.down")
        clear_series(context, "servers2.down")
        stop_reporter(pid1)
        stop_reporter(pid2)
      end
    end
  end

  @tag :capture_log
  test "notifying a UDP error and fetching a socket returns a new socket" do
    event = given_event_spec([:some, :event3])
    config = make_config(%{version: :v1, protocol: :udp}, %{events: [event], tags: %{}})
    start_reporter(config)
    udp = UDP.Connector.get_udp("default")
    Process.exit(udp.socket, :kill)

    assert eventually(fn ->
             new_udp = UDP.Connector.get_udp("default")
             new_udp != udp
           end)
  end

  test "events are not reported when reporter is shut down by its supervisor" do
    event_first = given_event_spec([:first, :event])
    event_second = given_event_spec([:second, :event])
    context = %{version: :v1, protocol: :udp}
    config = make_config(context, %{events: [event_first, event_second]})
    child_opts = [Map.to_list(config)]

    {:ok, supervisor} =
      Supervisor.start_link(
        [
          Supervisor.Spec.worker(TelemetryInfluxDB, child_opts)
        ],
        strategy: :one_for_one
      )

    Process.unlink(supervisor)

    Supervisor.stop(supervisor, :shutdown)

    assert :telemetry.list_handlers([:first, :event]) == []
    assert :telemetry.list_handlers([:second, :event]) == []

    :telemetry.execute([:first, :event], %{})
    :telemetry.execute([:second, :event], %{})

    refute_reported(context, "first.event")
    refute_reported(context, "second.event")
  end

  defp given_event_spec(name, metadata_tag_keys \\ []) do
    %{name: name, metadata_tag_keys: metadata_tag_keys}
  end

  defp start_reporter(config) do
    {:ok, pid} =
      config
      |> Map.to_list()
      |> TelemetryInfluxDB.start_link()

    pid
  end

  defp clear_series(context, name) do
    config = make_assertion_config(context)
    do_clear_series(config, name)

    eventually(fn ->
      empty_result?(config, query(config, name))
    end)
  end

  defp do_clear_series(%{version: :v1} = config, name) do
    q = "DROP SERIES FROM \"" <> name <> "\";"
    InfluxSimpleClient.V1.post(config, q)
  end

  defp do_clear_series(%{version: :v2} = config, name) do
    InfluxSimpleClient.V2.delete_measurement(config, name)
  end

  defp refute_reported(context, name) do
    config = make_assertion_config(context)
    res = query(config, name)
    assert empty_result?(config, res)
  end

  defp assert_reported(context, name, values, tags \\ %{}) do
    config = make_assertion_config(context)
    do_assert_reported(config, name, values, tags)
  end

  defp do_assert_reported(%{version: :v1} = config, name, values, tags) do
    assert record =
             eventually(fn ->
               res = query(config, name)

               with [inner_map] <- res["results"],
                    [record] <- inner_map["series"] do
                 record
               else
                 _ -> false
               end
             end)

    assert record["name"] == name
    assert record["columns"] == ["time"] ++ Enum.sort(Map.keys(values) ++ Map.keys(tags))
    map_vals = Map.values(values)
    map_tag_vals = Map.values(tags)
    all_vals = map_vals ++ map_tag_vals

    assert [[_ | tag_and_fields]] = record["values"]
    assert Enum.sort(tag_and_fields) == Enum.sort(all_vals)
    assert_tags(config, tags)
  end

  defp do_assert_reported(%{version: :v2} = config, name, values, tags) do
    results =
      eventually(fn ->
        res = query(config, name)

        if empty_result?(config, res) do
          false
        else
          res
        end
      end)

    assert_tags(config, name, tags, results)

    assert Enum.map(results, & &1["_field"]) == Map.keys(values)

    values
    |> Map.keys()
    |> Enum.each(&assert_value(&1, values, results))
  end

  defp assert_value(key, values, results) do
    field_result = Enum.find(results, &(&1["_field"] == key))

    assert field_result["_value"] == values[key]
  end

  defp assert_tags(_, %{}), do: :ok

  defp assert_tags(%{version: :v1} = config, tags) do
    assert eventually(fn ->
             res = InfluxSimpleClient.V1.query(config, "SHOW TAG KEYS;")

             with [inner_map] <- res["results"],
                  [record] <- inner_map["series"],
                  [tags] <- record["values"] do
               tags
             else
               _ -> false
             end
           end) == Map.keys(tags)
  end

  defp assert_tags(%{version: :v2}, name, tags, results) do
    expected_tag_values = Map.values(tags)
    expected_tag_keys = Map.keys(tags)

    assert Enum.all?(results, fn result ->
             measurement = result["_measurement"]

             has_tag_keys =
               Enum.all?(expected_tag_keys, fn tag_key -> Map.has_key?(result, tag_key) end)

             actual_tag_values =
               tags
               |> Map.keys()
               |> Enum.map(&result[&1])

             measurement == name and has_tag_keys and actual_tag_values == expected_tag_values
           end)
  end

  defp query(%{version: :v1} = config, name) do
    q = "SELECT * FROM \"" <> name <> "\";"
    InfluxSimpleClient.V1.query(config, q)
  end

  defp query(%{version: :v2, bucket: bucket} = config, name) do
    q = """
    from(bucket: "#{bucket}")
    |> range(start: -1m)
    |> filter(fn: (r) =>
      r._measurement == "#{name}"
    )
    |> group(columns: ["_field"])
    """

    res = InfluxSimpleClient.V2.query(config, q)

    FluxParser.parse_tables(res)
  end

  defp empty_result?(%{version: :v1}, %{"results" => [%{"statement_id" => 0}]}), do: true
  defp empty_result?(%{version: :v2}, res) when res == [], do: true
  defp empty_result?(_, _), do: false

  defp make_assertion_config(context, overrides \\ %{}) do
    make_config(%{context | protocol: :http}, overrides)
  end

  defp make_config(%{version: :v1, protocol: :udp}, overrides) do
    @v1_config
    |> Map.delete(:db)
    |> Map.merge(%{protocol: :udp, port: 8089})
    |> Map.merge(overrides)
  end

  defp make_config(%{version: :v1, protocol: :http}, overrides) do
    @v1_config
    |> Map.merge(%{protocol: :http, port: 8087})
    |> Map.merge(overrides)
  end

  defp make_config(%{version: :v2, protocol: :http}, overrides) do
    Map.merge(@v2_config, overrides)
  end

  defp wait_processes_to_die(pids) do
    eventually(fn -> Enum.all?(pids, fn p -> not Process.alive?(p) end) end)
  end

  defp stop_reporter(pid) do
    TelemetryInfluxDB.stop(pid)
  end
end
