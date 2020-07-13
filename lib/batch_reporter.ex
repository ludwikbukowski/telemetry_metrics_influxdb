defmodule TelemetryInfluxDB.BatchReporter do
  @moduledoc """
  This module handles batching events to be reported to Influx. It will call the provided
  report function with any events that have been enqueued since the last report. If a batch_size
  is not provided, the batch size will default to 1, which will send all events separately.
  """

  use GenServer

  defmodule State do
    defstruct [
      :report_fn,
      :batch_size,
      :config,
      report_scheduled?: false,
      unreported_events: []
    ]

    def enqueue_event(state, event, config) do
      %{state | unreported_events: state.unreported_events ++ [event], config: config}
    end

    def set_unreported_events(state, remaining_events) do
      %{state | unreported_events: remaining_events}
    end

    def set_report_scheduled(state) do
      %{state | report_scheduled?: true}
    end

    def reset_report_scheduled(state) do
      %{state | report_scheduled?: false}
    end
  end

  def start_link(opts \\ []) do
    {report_fn, opts} = Keyword.pop(opts, :report_fn)
    {batch_size, opts} = Keyword.pop(opts, :batch_size, 1)

    state = %State{report_fn: report_fn, batch_size: batch_size}

    GenServer.start_link(__MODULE__, state, opts)
  end

  def init(state) do
    {:ok, state}
  end

  def enqueue_event(pid \\ __MODULE__, event, config) do
    GenServer.cast(pid, {:enqueue_event, event, config})
  end

  def handle_cast({:enqueue_event, event, config}, state) do
    updated_state =
      state
      |> State.enqueue_event(event, config)
      |> maybe_report_events()

    {:noreply, updated_state}
  end

  def handle_info(:report_events, state) do
    {events_to_report, remaining_events} = Enum.split(state.unreported_events, state.batch_size)

    state.report_fn.(events_to_report, state.config)

    updated_state =
      state
      |> State.set_unreported_events(remaining_events)
      |> State.reset_report_scheduled()
      |> maybe_report_events()

    {:noreply, updated_state}
  end

  defp maybe_report_events(%{report_scheduled?: true} = state), do: state

  defp maybe_report_events(%{unreported_events: []} = state), do: state

  defp maybe_report_events(state) do
    send(self(), :report_events)
    State.set_report_scheduled(state)
  end
end
