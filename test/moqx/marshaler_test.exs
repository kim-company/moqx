defmodule MOQX.MarshalerTest do
  use ExUnit.Case, async: true

  alias MOQX.Common.{KeyValuePair, Location, Tuple}
  alias MOQX.Control
  alias MOQX.Data

  test "marshals tuple" do
    tuple = %Tuple{segments: ["a", "bb"]}
    assert MOQX.Marshaler.marshal(tuple) |> IO.iodata_to_binary() == <<0x02, 0x01, ?a, 0x02, ?b, ?b>>
  end

  test "marshals key-value pairs" do
    varint_kv = %KeyValuePair{type: 2, kind: :varint, value: 10}
    bytes_kv = %KeyValuePair{type: 1, kind: :bytes, value: "hi"}

    assert MOQX.Marshaler.marshal(varint_kv) |> IO.iodata_to_binary() == <<0x02, 0x0A>>
    assert MOQX.Marshaler.marshal(bytes_kv) |> IO.iodata_to_binary() == <<0x01, 0x02, ?h, ?i>>
  end

  test "marshals client_setup" do
    msg = %Control.ClientSetup{supported_versions: [1, 2], setup_parameters: []}

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() ==
             <<0x20, 0x00, 0x04, 0x02, 0x01, 0x02, 0x00>>
  end

  test "marshals subscribe with absolute range" do
    msg = %Control.Subscribe{
      request_id: 1,
      track_namespace: %Tuple{segments: ["ns"]},
      track_name: "t",
      subscriber_priority: 1,
      group_order: :ascending,
      forward: true,
      filter_type: :absolute_range,
      start_location: %Location{group: 5, object: 6},
      end_group: 7,
      parameters: []
    }

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() ==
             <<0x03, 0x00, 0x0F, 0x01, 0x01, 0x02, ?n, ?s, 0x01, ?t, 0x01, 0x01, 0x01, 0x04, 0x05,
               0x06, 0x07, 0x00>>
  end

  test "marshals publish_namespace" do
    msg = %Control.PublishNamespace{
      request_id: 9,
      track_namespace: %Tuple{segments: ["pub", "ns"]},
      parameters: []
    }

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() ==
             <<0x06, 0x00, 0x0A, 0x09, 0x02, 0x03, ?p, ?u, ?b, 0x02, ?n, ?s, 0x00>>
  end

  test "marshals datagram object" do
    msg = %Data.DatagramObject{
      track_alias: 1,
      group_id: 2,
      object_id: 3,
      publisher_priority: 4,
      extension_headers: nil,
      payload: "hi"
    }

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() ==
             <<0x00, 0x01, 0x02, 0x03, 0x04, 0x00, ?h, ?i>>
  end

  test "marshals subgroup header" do
    msg = %Data.SubgroupHeader{
      header_type: 0x14,
      track_alias: 1,
      group_id: 2,
      subgroup_id: 3,
      publisher_priority: 4
    }

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() == <<0x14, 0x01, 0x02, 0x03, 0x04>>
  end

  test "marshals subgroup object with payload" do
    msg = %Data.SubgroupObject{
      object_id: 5,
      previous_object_id: 4,
      has_extensions: false,
      payload: "hi"
    }

    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() == <<0x00, 0x02, ?h, ?i>>
  end

  test "marshals fetch header" do
    msg = %Data.FetchHeader{request_id: 9}
    assert MOQX.Marshaler.marshal(msg) |> IO.iodata_to_binary() == <<0x05, 0x09>>
  end
end
