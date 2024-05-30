defmodule TelemetryInfluxDB.Formatter do
  @moduledoc false

  @type tags() :: map()
  @type event_name() :: [atom()]
  @type event_measurements :: map()

  @spec format(event_name(), event_measurements, tags()) :: binary
  def format(event, measurements, tags \\ %{}) do
    Enum.join(event, ".") <> format_tags(tags) <> format_measurements(measurements)
  end

  defp format_measurements(measurements) do
    " " <> comma_separated(measurements)
  end

  defp format_tags(tags) do
    if tags != %{} do
      "," <> comma_separated(binary_map(tags))
    else
      ""
    end
  end

  defp comma_separated(measurements) do
    measurements
    |> Enum.map(fn {k, v} -> to_bin(k) <> "=" <> to_bin_quoted(v) end)
    |> Enum.join(",")
  end

  defp to_bin(val) when is_integer(val), do: Integer.to_string(val)
  defp to_bin(val) when is_float(val), do: Float.to_string(val)
  defp to_bin(val) when is_atom(val), do: Atom.to_string(val)
  defp to_bin(val) when is_map(val), do: "Unsupported data type"
  defp to_bin(val), do: escape_special_chars(val)

  defp to_bin_quoted(val) when is_integer(val), do: to_bin(val)
  defp to_bin_quoted(val) when is_float(val), do: to_bin(val)
  defp to_bin_quoted(val) when is_boolean(val), do: to_bin(val)
  defp to_bin_quoted(val) when is_map(val), do: "\"" <> to_bin(val) <> "\""
  defp to_bin_quoted(val), do: "\"" <> to_bin(val) <> "\""

  defp binary_map(atomized_map) do
    Map.new(atomized_map, fn {k, v} -> {to_bin(k), to_bin(v)} end)
  end

  # https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/
  defp escape_special_chars(string) do
    Regex.replace(~r/[=|,| |\"]/, string, fn a, _ -> "\\" <> a end)
  end
end
