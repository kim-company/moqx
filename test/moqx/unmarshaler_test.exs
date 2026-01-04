defmodule MOQX.UnmarshalerTest do
  use ExUnit.Case, async: true

  alias MOQX.Common.{KeyValuePair, Location, Tuple}
  alias MOQX.Control
  alias MOQX.Data
  alias MOQX.Unmarshaler

  defp encode(msg), do: msg |> MOQX.Marshaler.marshal() |> IO.iodata_to_binary()

  test "control unmarshaler decodes all control messages" do
    params = [%KeyValuePair{type: 2, kind: :varint, value: 10}]

    messages = [
      %Control.ClientSetup{supported_versions: [1, 2], setup_parameters: params},
      %Control.ServerSetup{selected_version: 1, setup_parameters: params},
      %Control.Goaway{new_session_uri: "https://example.com"},
      %Control.MaxRequestId{max_request_id: 9},
      %Control.RequestsBlocked{max_request_id: 10},
      %Control.Subscribe{
        request_id: 1,
        track_namespace: %Tuple{segments: ["ns"]},
        track_name: "t",
        subscriber_priority: 1,
        group_order: :ascending,
        forward: true,
        filter_type: :absolute_start,
        start_location: %Location{group: 5, object: 6},
        end_group: nil,
        parameters: params
      },
      %Control.SubscribeOk{
        request_id: 1,
        track_alias: 7,
        expires: 0,
        group_order: :ascending,
        content_exists: true,
        largest_location: %Location{group: 9, object: 10},
        parameters: params
      },
      %Control.SubscribeError{request_id: 1, error_code: 0x2, reason_phrase: "nope"},
      %Control.SubscribeUpdate{
        request_id: 2,
        subscription_request_id: 1,
        start_location: %Location{group: 1, object: 2},
        end_group: 3,
        subscriber_priority: 4,
        forward: true,
        parameters: params
      },
      %Control.Unsubscribe{request_id: 1},
      %Control.Fetch{
        request_id: 3,
        subscriber_priority: 1,
        group_order: :ascending,
        fetch_type: :standalone,
        standalone: %Control.FetchStandaloneProps{
          track_namespace: %Tuple{segments: ["ns"]},
          track_name: "t",
          start_location: %Location{group: 1, object: 1},
          end_location: %Location{group: 2, object: 2}
        },
        joining: nil,
        parameters: params
      },
      %Control.FetchOk{
        request_id: 3,
        group_order: :ascending,
        end_of_track: false,
        end_location: %Location{group: 5, object: 6},
        parameters: params
      },
      %Control.FetchError{request_id: 3, error_code: 0x1, reason_phrase: "unauthorized"},
      %Control.FetchCancel{request_id: 3},
      %Control.TrackStatus{
        request_id: 4,
        track_namespace: %Tuple{segments: ["ns"]},
        track_name: "t",
        subscriber_priority: 1,
        group_order: :ascending,
        forward: false,
        filter_type: :latest_object,
        start_location: nil,
        end_group: nil,
        parameters: params
      },
      %Control.TrackStatusOk{
        request_id: 4,
        track_alias: 5,
        expires: 1,
        group_order: :ascending,
        content_exists: false,
        largest_location: nil,
        parameters: params
      },
      %Control.TrackStatusError{request_id: 4, error_code: 0x5, reason_phrase: "bad"},
      %Control.PublishNamespace{request_id: 5, track_namespace: %Tuple{segments: ["pub"]}, parameters: params},
      %Control.PublishNamespaceOk{request_id: 5},
      %Control.PublishNamespaceError{request_id: 5, error_code: 0x1, reason_phrase: "oops"},
      %Control.PublishNamespaceDone{track_namespace: %Tuple{segments: ["pub"]}},
      %Control.PublishNamespaceCancel{track_namespace: %Tuple{segments: ["pub"]}, error_code: 0x1, reason_phrase: "done"},
      %Control.SubscribeNamespace{request_id: 6, track_namespace_prefix: %Tuple{segments: ["pre"]}, parameters: params},
      %Control.SubscribeNamespaceOk{request_id: 6},
      %Control.SubscribeNamespaceError{request_id: 6, error_code: 0x1, reason_phrase: "no"},
      %Control.UnsubscribeNamespace{track_namespace_prefix: %Tuple{segments: ["pre"]}},
      %Control.Publish{
        request_id: 7,
        track_namespace: %Tuple{segments: ["ns"]},
        track_name: "t",
        track_alias: 1,
        group_order: :ascending,
        content_exists: true,
        largest_location: %Location{group: 1, object: 1},
        forward: false,
        parameters: params
      },
      %Control.PublishOk{
        request_id: 7,
        forward: false,
        subscriber_priority: 1,
        group_order: :ascending,
        filter_type: :next_group_start,
        start_location: nil,
        end_group: nil,
        parameters: params
      },
      %Control.PublishError{request_id: 7, error_code: 0x1, reason_phrase: "fail"},
      %Control.PublishDone{request_id: 7, status_code: 0x3, stream_count: 0, reason_phrase: "done"}
    ]

    state = Unmarshaler.init(:control)

    Enum.each(messages, fn msg ->
      data = encode(msg)
      assert {:ok, [{:control, decoded}], state} = Unmarshaler.feed(state, data)
      assert decoded == msg
      assert state.buffer == <<>>
    end)
  end

  test "control unmarshaler handles partial frames" do
    msg = %Control.MaxRequestId{max_request_id: 123}
    data = encode(msg)
    {first, second} = :erlang.split_binary(data, div(byte_size(data), 2))

    state = Unmarshaler.init(:control)
    assert {:need_more_data, state} = Unmarshaler.feed(state, first)
    assert {:ok, [{:control, decoded}], state} = Unmarshaler.feed(state, second)
    assert decoded == msg
    assert state.buffer == <<>>
  end

  test "datagram unmarshaler handles partial" do
    msg = %Data.DatagramObject{
      track_alias: 1,
      group_id: 2,
      object_id: 3,
      publisher_priority: 4,
      extension_headers: nil,
      payload: "hi"
    }

    data = encode(msg)
    {first, second} = :erlang.split_binary(data, 3)

    state = Unmarshaler.init(:datagram)
    assert {:need_more_data, state} = Unmarshaler.feed(state, first)
    assert {:ok, [{:datagram, decoded}], state} = Unmarshaler.feed(state, second)
    assert decoded == msg
    assert state.buffer == <<>>
  end

  test "subgroup stream unmarshaler decodes header and object" do
    header = %Data.SubgroupHeader{
      header_type: 0x14,
      track_alias: 1,
      group_id: 2,
      subgroup_id: 3,
      publisher_priority: 4
    }

    object = %Data.SubgroupObject{
      object_id: 10,
      previous_object_id: nil,
      has_extensions: false,
      payload: "hi"
    }

    data = encode(header) <> encode(object)

    state = Unmarshaler.init(:subgroup)
    assert {:ok, events, state} = Unmarshaler.feed(state, data)
    assert [{:subgroup_header, decoded_header}, {:subgroup_object, decoded_object}] = events
    assert decoded_header == header
    assert decoded_object.object_id == object.object_id
    assert decoded_object.payload == object.payload
    assert state.buffer == <<>>
  end

  test "fetch stream unmarshaler decodes header and object" do
    header = %Data.FetchHeader{request_id: 9}

    object = %Data.FetchObject{
      group_id: 1,
      subgroup_id: 2,
      object_id: 3,
      publisher_priority: 4,
      extension_headers: nil,
      payload: "hi"
    }

    data = encode(header) <> encode(object)

    state = Unmarshaler.init(:fetch)
    assert {:ok, events, state} = Unmarshaler.feed(state, data)
    assert [{:fetch_header, decoded_header}, {:fetch_object, decoded_object}] = events
    assert decoded_header == header
    assert decoded_object.group_id == object.group_id
    assert decoded_object.payload == object.payload
    assert state.buffer == <<>>
  end
end
