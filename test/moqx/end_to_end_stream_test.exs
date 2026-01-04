defmodule MOQX.EndToEndStreamTest do
  use ExUnit.Case, async: true

  alias MOQX.Common.KeyValuePair
  alias MOQX.Data
  alias MOQX.Unmarshaler

  defp encode(msg), do: msg |> MOQX.Marshaler.marshal() |> IO.iodata_to_binary()

  defp chunk_binary(binary, seed \\ {1, 2, 3}) do
    :rand.seed(:exsplus, seed)
    do_chunk(binary, [])
  end

  defp do_chunk(<<>>, acc), do: Enum.reverse(acc)

  defp do_chunk(binary, acc) do
    size = :rand.uniform(7)
    size = min(size, byte_size(binary))
    <<chunk::binary-size(size), rest::binary>> = binary
    do_chunk(rest, [chunk | acc])
  end

  defp feed_all(state, chunks) do
    Enum.reduce(chunks, {state, []}, fn chunk, {state, acc} ->
      case Unmarshaler.feed(state, chunk) do
        {:ok, events, state} -> {state, acc ++ events}
        {:need_more_data, state} -> {state, acc}
        {:error, reason, _state} -> flunk("unmarshal error: #{inspect(reason)}")
      end
    end)
  end

  test "subgroup stream decode with random chunking" do
    header = %Data.SubgroupHeader{
      header_type: 0x1D,
      track_alias: 1,
      group_id: 7,
      subgroup_id: 3,
      publisher_priority: 42
    }

    extension = %KeyValuePair{type: 1, kind: :bytes, value: "x"}

    obj1 = %Data.SubgroupObject{
      object_id: 10,
      previous_object_id: nil,
      has_extensions: true,
      extension_headers: [extension],
      payload: "aa"
    }

    obj2 = %Data.SubgroupObject{
      object_id: 11,
      previous_object_id: 10,
      has_extensions: true,
      extension_headers: nil,
      payload: "bb"
    }

    obj3 = %Data.SubgroupObject{
      object_id: 12,
      previous_object_id: 11,
      has_extensions: true,
      extension_headers: nil,
      object_status: :end_of_group,
      payload: nil
    }

    stream = encode(header) <> encode(obj1) <> encode(obj2) <> encode(obj3)
    chunks = chunk_binary(stream)

    state = Unmarshaler.init(:subgroup)
    {state, events} = feed_all(state, chunks)

    assert state.buffer == <<>>

    assert [
             {:subgroup_header, decoded_header},
             {:subgroup_object, decoded_obj1},
             {:subgroup_object, decoded_obj2},
             {:subgroup_object, decoded_obj3}
           ] = events

    assert decoded_header == header
    assert decoded_obj1.object_id == obj1.object_id
    assert decoded_obj1.payload == obj1.payload
    assert decoded_obj1.extension_headers == obj1.extension_headers

    assert decoded_obj2.object_id == obj2.object_id
    assert decoded_obj2.payload == obj2.payload

    assert decoded_obj3.object_id == obj3.object_id
    assert decoded_obj3.object_status == obj3.object_status
  end
end
