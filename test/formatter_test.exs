defmodule TelemetryInfluxDB.FormatterTest do
  use ExUnit.Case, async: true
  alias TelemetryInfluxDB.Formatter

  test "formats the point given atomized fields map" do
    assert Formatter.format([:sunny, :day], %{temperature: 30, wind: :low}) ==
             "sunny.day temperature=30,wind=\"low\""
  end

  test "formats the point given binary fields map" do
    assert Formatter.format([:windy, :day], %{"temperature" => 20, "wind" => "high"}) ==
             "windy.day temperature=20,wind=\"high\""
  end

  test "formats the point given atomized tags" do
    assert Formatter.format([:rainy, :day], %{"temperature" => 13, "wind" => "medium"}, %{
             topic: :weather
           }) ==
             "rainy.day,topic=weather temperature=13,wind=\"medium\""
  end

  test "formats the point given binary tags" do
    assert Formatter.format([:snowy, :day], %{"temperature" => -5, "wind" => "medium"}, %{
             "topic" => "weather"
           }) ==
             "snowy.day,topic=weather temperature=-5,wind=\"medium\""
  end

  test "formats the point with multiple tags and multiple fields" do
    assert Formatter.format([:event], %{"field1" => "field1_val", "field2" => "field2_val"}, %{
             "tag1" => "tag1_val",
             "tag2" => "tag2_val"
           }) ==
             "event,tag1=tag1_val,tag2=tag2_val field1=\"field1_val\",field2=\"field2_val\""
  end

  test "properly formats the point with integer" do
    assert Formatter.format([:integer, :test], %{my_integer: 110}) ==
             "integer.test my_integer=110"
  end

  test "properly formats the point with float" do
    assert Formatter.format([:float, :test], %{my_float: 0.31}) ==
             "float.test my_float=0.31"
  end

  test "properly formats the point with string" do
    assert Formatter.format([:string, :test], %{my_string: "jacknicholson"}) ==
             "string.test my_string=\"jacknicholson\""
  end

  test "properly formats the point with map" do
    assert Formatter.format([:map, :test], %{my_map: %{key1: "jacknicholson"}}) ==
             "map.test my_map=\"Unsupported data type\""
  end

  test "properly formats the point with string tag that looks like integer though" do
    assert Formatter.format([:string, :test], %{my_fake_int: "123"}) ==
             "string.test my_fake_int=\"123\""
  end

  test "properly formats the point with boolean" do
    assert Formatter.format([:boolean, :test], %{my_boolean: true}) ==
             "boolean.test my_boolean=true"
  end

  test "properly formats special characters" do
    assert Formatter.format([:special, :coma], %{field: ",coma"}) ==
             "special.coma field=\"\\,coma\""

    assert Formatter.format([:special, :equals], %{field: "e=quals"}) ==
             "special.equals field=\"e\\=quals\""

    assert Formatter.format([:special, :space], %{field: "my space"}) ==
             "special.space field=\"my\\ space\""

    assert Formatter.format([:special, :quote], %{field: "quote\""}) ==
             "special.quote field=\"quote\\\"\""
  end
end
