defmodule MOQX.Unmarshaler do
  @moduledoc """
  Stateful MOQT unmarshaler that works for control, datagram, and stream data.
  """

  defstruct mode: :control,
            buffer: <<>>,
            phase: :need_header,
            header: nil,
            prev_object_id: nil,
            has_extensions: false

  @type mode :: :control | :datagram | :subgroup | :fetch
  @type event ::
          {:control, MOQX.Control.t()}
          | {:datagram, MOQX.Data.DatagramObject.t()}
          | {:subgroup_header, MOQX.Data.SubgroupHeader.t()}
          | {:subgroup_object, MOQX.Data.SubgroupObject.t()}
          | {:fetch_header, MOQX.Data.FetchHeader.t()}
          | {:fetch_object, MOQX.Data.FetchObject.t()}

  @type t :: %__MODULE__{
          mode: mode(),
          buffer: binary(),
          phase: atom(),
          header: term(),
          prev_object_id: non_neg_integer() | nil,
          has_extensions: boolean()
        }

  @spec init(mode()) :: t()
  @doc "Initialize a decoder for a single control/data stream or datagram flow."
  def init(mode) when mode in [:control, :datagram, :subgroup, :fetch] do
    %__MODULE__{mode: mode}
  end

  @spec feed(t(), binary()) :: {:ok, [event()], t()} | {:need_more_data, t()} | {:error, any(), t()}
  @doc "Feed a chunk of bytes and emit any decoded events."
  def feed(%__MODULE__{} = state, bytes) when is_binary(bytes) do
    buffer = state.buffer <> bytes

    case state.mode do
      :control ->
        decode_control(state, buffer, [])

      :datagram ->
        decode_datagram(state, buffer, [])

      :subgroup ->
        decode_subgroup(state, buffer, [])

      :fetch ->
        decode_fetch(state, buffer, [])
    end
  end

  defp decode_control(state, buffer, events) do
    case parse_control_frame(buffer) do
      {:ok, msg, rest} ->
        decode_control(%{state | buffer: rest}, rest, events ++ [{:control, msg}])

      :need_more_data ->
        return_events_or_need_more(state, buffer, events)

      {:error, reason} ->
        {:error, reason, %{state | buffer: buffer}}
    end
  end

  defp decode_datagram(state, buffer, events) do
    case parse_datagram(buffer) do
      {:ok, msg, rest} ->
        decode_datagram(%{state | buffer: rest}, rest, events ++ [{:datagram, msg}])

      :need_more_data ->
        return_events_or_need_more(state, buffer, events)

      {:error, reason} ->
        {:error, reason, %{state | buffer: buffer}}
    end
  end

  defp decode_subgroup(state, buffer, events) do
    case state.phase do
      :need_header ->
        case parse_subgroup_header(buffer) do
          {:ok, header, rest, has_extensions} ->
            next_state = %{
              state
              | buffer: rest,
                phase: :need_object,
                header: header,
                has_extensions: has_extensions,
                prev_object_id: nil
            }

            decode_subgroup(next_state, rest, events ++ [{:subgroup_header, header}])

          :need_more_data ->
            {:need_more_data, %{state | buffer: buffer}}

          {:error, reason} ->
            {:error, reason, %{state | buffer: buffer}}
        end

      :need_object ->
        case parse_subgroup_object(buffer, state.prev_object_id, state.has_extensions) do
          {:ok, object, rest, object_id} ->
            next_state = %{state | buffer: rest, prev_object_id: object_id}
            decode_subgroup(next_state, rest, events ++ [{:subgroup_object, object}])

          :need_more_data ->
            return_events_or_need_more(state, buffer, events)

          {:error, reason} ->
            {:error, reason, %{state | buffer: buffer}}
        end
    end
  end

  defp decode_fetch(state, buffer, events) do
    case state.phase do
      :need_header ->
        case parse_fetch_header(buffer) do
          {:ok, header, rest} ->
            next_state = %{
              state
              | buffer: rest,
                phase: :need_object,
                header: header,
                prev_object_id: nil
            }

            decode_fetch(next_state, rest, events ++ [{:fetch_header, header}])

          :need_more_data ->
            {:need_more_data, %{state | buffer: buffer}}

          {:error, reason} ->
            {:error, reason, %{state | buffer: buffer}}
        end

      :need_object ->
        case parse_fetch_object(buffer) do
          {:ok, object, rest} ->
            next_state = %{state | buffer: rest}
            decode_fetch(next_state, rest, events ++ [{:fetch_object, object}])

          :need_more_data ->
            return_events_or_need_more(state, buffer, events)

          {:error, reason} ->
            {:error, reason, %{state | buffer: buffer}}
        end
    end
  end

  defp parse_control_frame(buffer) do
    with {:ok, type, rest} <- MOQX.Varint.decode(buffer),
         {:ok, payload_len, rest} <- decode_u16(rest),
         {:ok, payload, rest} <- split_binary(rest, payload_len),
         {:ok, msg} <- decode_control_payload(type, payload) do
      {:ok, msg, rest}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_datagram(buffer) do
    with {:ok, msg_type, rest} <- MOQX.Varint.decode(buffer),
         true <- msg_type in [0x00, 0x01] || {:error, :invalid_datagram_type},
         {:ok, track_alias, rest} <- MOQX.Varint.decode(rest),
         {:ok, group_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, object_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, publisher_priority, rest} <- decode_u8(rest),
         {:ok, ext_len, rest} <- MOQX.Varint.decode(rest),
         {:ok, ext_bytes, rest} <- split_binary(rest, ext_len),
         {:ok, extensions} <- decode_extensions(ext_bytes, msg_type),
         {:ok, payload, rest} <- split_binary(rest, byte_size(rest)) do
      {:ok,
       %MOQX.Data.DatagramObject{
         track_alias: track_alias,
         group_id: group_id,
         object_id: object_id,
         publisher_priority: publisher_priority,
         extension_headers: extensions,
         payload: payload
       }, rest}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_subgroup_header(buffer) do
    with {:ok, type, rest} <- MOQX.Varint.decode(buffer),
         true <- type in [0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D] ||
           {:error, :invalid_subgroup_header_type},
         {:ok, track_alias, rest} <- MOQX.Varint.decode(rest),
         {:ok, group_id, rest} <- MOQX.Varint.decode(rest),
         {subgroup_id, rest} <- decode_subgroup_id(type, rest),
         {:ok, publisher_priority, rest} <- decode_u8(rest) do
      has_extensions = subgroup_type_has_extensions?(type)

      header = %MOQX.Data.SubgroupHeader{
        header_type: type,
        track_alias: track_alias,
        group_id: group_id,
        subgroup_id: subgroup_id,
        publisher_priority: publisher_priority
      }

      {:ok, header, rest, has_extensions}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_subgroup_object(buffer, prev_object_id, has_extensions) do
    with {:ok, delta, rest} <- MOQX.Varint.decode(buffer),
         object_id <- compute_object_id(prev_object_id, delta),
         {:ok, extensions, rest} <- decode_object_extensions(rest, has_extensions),
         {:ok, payload_len, rest} <- MOQX.Varint.decode(rest),
         {:ok, payload, status, rest} <- decode_payload_or_status(rest, payload_len) do
      object = %MOQX.Data.SubgroupObject{
        object_id: object_id,
        previous_object_id: prev_object_id,
        has_extensions: has_extensions,
        extension_headers: extensions,
        object_status: status,
        payload: payload
      }

      {:ok, object, rest, object_id}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_fetch_header(buffer) do
    with {:ok, header_type, rest} <- MOQX.Varint.decode(buffer),
         true <- header_type == 0x05 || {:error, :invalid_fetch_header_type},
         {:ok, request_id, rest} <- MOQX.Varint.decode(rest) do
      {:ok, %MOQX.Data.FetchHeader{request_id: request_id}, rest}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_fetch_object(buffer) do
    with {:ok, group_id, rest} <- MOQX.Varint.decode(buffer),
         {:ok, subgroup_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, object_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, publisher_priority, rest} <- decode_u8(rest),
         {:ok, ext_len, rest} <- MOQX.Varint.decode(rest),
         {:ok, ext_bytes, rest} <- split_binary(rest, ext_len),
         {:ok, extensions} <- decode_extensions(ext_bytes, 0x01),
         {:ok, payload_len, rest} <- MOQX.Varint.decode(rest),
         {:ok, payload, status, rest} <- decode_payload_or_status(rest, payload_len) do
      {:ok,
       %MOQX.Data.FetchObject{
         group_id: group_id,
         subgroup_id: subgroup_id,
         object_id: object_id,
         publisher_priority: publisher_priority,
         extension_headers: extensions,
         object_status: status,
         payload: payload
       }, rest}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_control_payload(0x20, payload), do: decode_client_setup(payload)
  defp decode_control_payload(0x21, payload), do: decode_server_setup(payload)
  defp decode_control_payload(0x10, payload), do: decode_goaway(payload)
  defp decode_control_payload(0x15, payload), do: decode_max_request_id(payload)
  defp decode_control_payload(0x1A, payload), do: decode_requests_blocked(payload)
  defp decode_control_payload(0x03, payload), do: decode_subscribe(payload)
  defp decode_control_payload(0x04, payload), do: decode_subscribe_ok(payload)
  defp decode_control_payload(0x05, payload), do: decode_subscribe_error(payload)
  defp decode_control_payload(0x02, payload), do: decode_subscribe_update(payload)
  defp decode_control_payload(0x0A, payload), do: decode_unsubscribe(payload)
  defp decode_control_payload(0x16, payload), do: decode_fetch(payload)
  defp decode_control_payload(0x18, payload), do: decode_fetch_ok(payload)
  defp decode_control_payload(0x19, payload), do: decode_fetch_error(payload)
  defp decode_control_payload(0x17, payload), do: decode_fetch_cancel(payload)
  defp decode_control_payload(0x0D, payload), do: decode_track_status(payload)
  defp decode_control_payload(0x0E, payload), do: decode_track_status_ok(payload)
  defp decode_control_payload(0x0F, payload), do: decode_track_status_error(payload)
  defp decode_control_payload(0x06, payload), do: decode_publish_namespace(payload)
  defp decode_control_payload(0x07, payload), do: decode_publish_namespace_ok(payload)
  defp decode_control_payload(0x08, payload), do: decode_publish_namespace_error(payload)
  defp decode_control_payload(0x09, payload), do: decode_publish_namespace_done(payload)
  defp decode_control_payload(0x0C, payload), do: decode_publish_namespace_cancel(payload)
  defp decode_control_payload(0x11, payload), do: decode_subscribe_namespace(payload)
  defp decode_control_payload(0x12, payload), do: decode_subscribe_namespace_ok(payload)
  defp decode_control_payload(0x13, payload), do: decode_subscribe_namespace_error(payload)
  defp decode_control_payload(0x14, payload), do: decode_unsubscribe_namespace(payload)
  defp decode_control_payload(0x1D, payload), do: decode_publish(payload)
  defp decode_control_payload(0x1E, payload), do: decode_publish_ok(payload)
  defp decode_control_payload(0x1F, payload), do: decode_publish_error(payload)
  defp decode_control_payload(0x0B, payload), do: decode_publish_done(payload)
  defp decode_control_payload(_, _payload), do: {:error, :unknown_control_type}

  defp decode_client_setup(payload) do
    with {:ok, count, rest} <- MOQX.Varint.decode(payload),
         {:ok, versions, rest} <- decode_varint_list(rest, count),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.ClientSetup{supported_versions: versions, setup_parameters: params}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_server_setup(payload) do
    with {:ok, version, rest} <- MOQX.Varint.decode(payload),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.ServerSetup{selected_version: version, setup_parameters: params}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_goaway(payload) do
    with {:ok, length, rest} <- MOQX.Varint.decode(payload),
         {:ok, uri, rest} <- take_bytes(rest, length),
         true <- rest == <<>> || {:error, :extra_data} do
      uri_val = if length == 0, do: nil, else: uri
      {:ok, %MOQX.Control.Goaway{new_session_uri: uri_val}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_max_request_id(payload) do
    with {:ok, max_request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.MaxRequestId{max_request_id: max_request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_requests_blocked(payload) do
    with {:ok, max_request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.RequestsBlocked{max_request_id: max_request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_namespace, rest} <- decode_tuple(rest),
         {:ok, track_name, rest} <- decode_string(rest),
         {:ok, subscriber_priority, rest} <- decode_u8(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         {:ok, forward, rest} <- decode_bool(rest),
         {:ok, filter_type, rest} <- decode_filter_type(rest),
         {:ok, start_location, end_group, rest} <- decode_filter_range(rest, filter_type),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.Subscribe{
         request_id: request_id,
         track_namespace: track_namespace,
         track_name: track_name,
         subscriber_priority: subscriber_priority,
         group_order: group_order,
         forward: forward,
         filter_type: filter_type,
         start_location: start_location,
         end_group: end_group,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_alias, rest} <- MOQX.Varint.decode(rest),
         {:ok, expires, rest} <- MOQX.Varint.decode(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         true <- group_order != :original || {:error, :invalid_group_order},
         {:ok, content_exists, rest} <- decode_bool(rest),
         {:ok, largest_location, rest} <- decode_optional_location(rest, content_exists),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.SubscribeOk{
         request_id: request_id,
         track_alias: track_alias,
         expires: expires,
         group_order: group_order,
         content_exists: content_exists,
         largest_location: largest_location,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.SubscribeError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_update(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, subscription_request_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, start_location, rest} <- decode_location(rest),
         {:ok, end_group, rest} <- MOQX.Varint.decode(rest),
         {:ok, subscriber_priority, rest} <- decode_u8(rest),
         {:ok, forward, rest} <- decode_bool(rest),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.SubscribeUpdate{
         request_id: request_id,
         subscription_request_id: subscription_request_id,
         start_location: start_location,
         end_group: end_group,
         subscriber_priority: subscriber_priority,
         forward: forward,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_unsubscribe(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.Unsubscribe{request_id: request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_fetch(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, subscriber_priority, rest} <- decode_u8(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         {:ok, fetch_type, rest} <- decode_fetch_type(rest),
         {:ok, standalone, joining, rest} <- decode_fetch_fields(rest, fetch_type),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.Fetch{
         request_id: request_id,
         subscriber_priority: subscriber_priority,
         group_order: group_order,
         fetch_type: fetch_type,
         standalone: standalone,
         joining: joining,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_fetch_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, group_order, rest} <- decode_group_order(rest),
         true <- group_order != :original || {:error, :invalid_group_order},
         {:ok, end_of_track, rest} <- decode_bool(rest),
         {:ok, end_location, rest} <- decode_location(rest),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.FetchOk{
         request_id: request_id,
         group_order: group_order,
         end_of_track: end_of_track,
         end_location: end_location,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_fetch_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.FetchError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_fetch_cancel(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.FetchCancel{request_id: request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_track_status(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_namespace, rest} <- decode_tuple(rest),
         {:ok, track_name, rest} <- decode_string(rest),
         {:ok, subscriber_priority, rest} <- decode_u8(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         {:ok, forward, rest} <- decode_bool(rest),
         {:ok, filter_type, rest} <- decode_filter_type(rest),
         {:ok, start_location, end_group, rest} <- decode_filter_range(rest, filter_type),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.TrackStatus{
         request_id: request_id,
         track_namespace: track_namespace,
         track_name: track_name,
         subscriber_priority: subscriber_priority,
         group_order: group_order,
         forward: forward,
         filter_type: filter_type,
         start_location: start_location,
         end_group: end_group,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_track_status_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_alias, rest} <- MOQX.Varint.decode(rest),
         {:ok, expires, rest} <- MOQX.Varint.decode(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         true <- group_order != :original || {:error, :invalid_group_order},
         {:ok, content_exists, rest} <- decode_bool(rest),
         {:ok, largest_location, rest} <- decode_optional_location(rest, content_exists),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.TrackStatusOk{
         request_id: request_id,
         track_alias: track_alias,
         expires: expires,
         group_order: group_order,
         content_exists: content_exists,
         largest_location: largest_location,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_track_status_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.TrackStatusError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_namespace(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_namespace, rest} <- decode_tuple(rest),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.PublishNamespace{request_id: request_id, track_namespace: track_namespace, parameters: params}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_namespace_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.PublishNamespaceOk{request_id: request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_namespace_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.PublishNamespaceError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_namespace_done(payload) do
    with {:ok, track_namespace, rest} <- decode_tuple(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.PublishNamespaceDone{track_namespace: track_namespace}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_namespace_cancel(payload) do
    with {:ok, track_namespace, rest} <- decode_tuple(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.PublishNamespaceCancel{
         track_namespace: track_namespace,
         error_code: error_code,
         reason_phrase: reason
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_namespace(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_namespace_prefix, rest} <- decode_tuple(rest),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.SubscribeNamespace{
         request_id: request_id,
         track_namespace_prefix: track_namespace_prefix,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_namespace_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.SubscribeNamespaceOk{request_id: request_id}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_subscribe_namespace_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.SubscribeNamespaceError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_unsubscribe_namespace(payload) do
    with {:ok, track_namespace_prefix, rest} <- decode_tuple(payload),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.UnsubscribeNamespace{track_namespace_prefix: track_namespace_prefix}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, track_namespace, rest} <- decode_tuple(rest),
         {:ok, track_name, rest} <- decode_string(rest),
         {:ok, track_alias, rest} <- MOQX.Varint.decode(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         {:ok, content_exists, rest} <- decode_bool(rest),
         {:ok, largest_location, rest} <- decode_optional_location(rest, content_exists),
         {:ok, forward, rest} <- decode_bool(rest),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.Publish{
         request_id: request_id,
         track_namespace: track_namespace,
         track_name: track_name,
         track_alias: track_alias,
         group_order: group_order,
         content_exists: content_exists,
         largest_location: largest_location,
         forward: forward,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_ok(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, forward, rest} <- decode_bool(rest),
         {:ok, subscriber_priority, rest} <- decode_u8(rest),
         {:ok, group_order, rest} <- decode_group_order(rest),
         {:ok, filter_type, rest} <- decode_filter_type(rest),
         {:ok, start_location, end_group, rest} <- decode_filter_range(rest, filter_type),
         {:ok, params, rest} <- decode_params(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.PublishOk{
         request_id: request_id,
         forward: forward,
         subscriber_priority: subscriber_priority,
         group_order: group_order,
         filter_type: filter_type,
         start_location: start_location,
         end_group: end_group,
         parameters: params
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_error(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, error_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok, %MOQX.Control.PublishError{request_id: request_id, error_code: error_code, reason_phrase: reason}}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_publish_done(payload) do
    with {:ok, request_id, rest} <- MOQX.Varint.decode(payload),
         {:ok, status_code, rest} <- MOQX.Varint.decode(rest),
         {:ok, stream_count, rest} <- MOQX.Varint.decode(rest),
         {:ok, reason, rest} <- decode_reason(rest),
         true <- rest == <<>> || {:error, :extra_data} do
      {:ok,
       %MOQX.Control.PublishDone{
         request_id: request_id,
         status_code: status_code,
         stream_count: stream_count,
         reason_phrase: reason
       }}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_u8(<<value::8, rest::binary>>), do: {:ok, value, rest}
  defp decode_u8(_), do: :need_more_data

  defp decode_u16(<<value::16, rest::binary>>), do: {:ok, value, rest}
  defp decode_u16(_), do: :need_more_data

  defp decode_bool(buffer) do
    case decode_u8(buffer) do
      {:ok, 0, rest} -> {:ok, false, rest}
      {:ok, 1, rest} -> {:ok, true, rest}
      {:ok, _, _} -> {:error, :invalid_bool}
      :need_more_data -> :need_more_data
    end
  end

  defp decode_group_order(buffer) do
    case decode_u8(buffer) do
      {:ok, 0x0, rest} -> {:ok, :original, rest}
      {:ok, 0x1, rest} -> {:ok, :ascending, rest}
      {:ok, 0x2, rest} -> {:ok, :descending, rest}
      {:ok, _, _} -> {:error, :invalid_group_order}
      :need_more_data -> :need_more_data
    end
  end

  defp decode_filter_type(buffer) do
    case MOQX.Varint.decode(buffer) do
      {:ok, 0x1, rest} -> {:ok, :next_group_start, rest}
      {:ok, 0x2, rest} -> {:ok, :latest_object, rest}
      {:ok, 0x3, rest} -> {:ok, :absolute_start, rest}
      {:ok, 0x4, rest} -> {:ok, :absolute_range, rest}
      {:ok, _, _} -> {:error, :invalid_filter_type}
      :need_more_data -> :need_more_data
    end
  end

  defp decode_fetch_type(buffer) do
    case MOQX.Varint.decode(buffer) do
      {:ok, 0x1, rest} -> {:ok, :standalone, rest}
      {:ok, 0x2, rest} -> {:ok, :relative_fetch, rest}
      {:ok, 0x3, rest} -> {:ok, :absolute_fetch, rest}
      {:ok, _, _} -> {:error, :invalid_fetch_type}
      :need_more_data -> :need_more_data
    end
  end

  defp decode_filter_range(rest, :absolute_start) do
    with {:ok, location, rest} <- decode_location(rest) do
      {:ok, location, nil, rest}
    end
  end

  defp decode_filter_range(rest, :absolute_range) do
    with {:ok, location, rest} <- decode_location(rest),
         {:ok, end_group, rest} <- MOQX.Varint.decode(rest) do
      {:ok, location, end_group, rest}
    end
  end

  defp decode_filter_range(rest, _), do: {:ok, nil, nil, rest}

  defp decode_fetch_fields(rest, :standalone) do
    with {:ok, track_namespace, rest} <- decode_tuple(rest),
         {:ok, track_name, rest} <- decode_string(rest),
         {:ok, start_location, rest} <- decode_location(rest),
         {:ok, end_location, rest} <- decode_location(rest) do
      {:ok,
       %MOQX.Control.FetchStandaloneProps{
         track_namespace: track_namespace,
         track_name: track_name,
         start_location: start_location,
         end_location: end_location
       }, nil, rest}
    end
  end

  defp decode_fetch_fields(rest, _joining_type) do
    with {:ok, joining_request_id, rest} <- MOQX.Varint.decode(rest),
         {:ok, joining_start, rest} <- MOQX.Varint.decode(rest) do
      {:ok, nil, %MOQX.Control.FetchJoiningProps{joining_request_id: joining_request_id, joining_start: joining_start}, rest}
    end
  end

  defp decode_optional_location(rest, false), do: {:ok, nil, rest}
  defp decode_optional_location(rest, true), do: decode_location(rest)

  defp decode_location(rest) do
    with {:ok, group, rest} <- MOQX.Varint.decode(rest),
         {:ok, object, rest} <- MOQX.Varint.decode(rest) do
      {:ok, %MOQX.Common.Location{group: group, object: object}, rest}
    end
  end

  defp decode_tuple(rest) do
    with {:ok, count, rest} <- MOQX.Varint.decode(rest) do
      decode_tuple_fields(rest, count, [])
    end
  end

  defp decode_tuple_fields(rest, 0, acc) do
    {:ok, %MOQX.Common.Tuple{segments: Enum.reverse(acc)}, rest}
  end

  defp decode_tuple_fields(rest, count, acc) do
    with {:ok, segment, rest} <- decode_bytes(rest) do
      decode_tuple_fields(rest, count - 1, [segment | acc])
    end
  end

  defp decode_bytes(rest) do
    with {:ok, length, rest} <- MOQX.Varint.decode(rest),
         {:ok, bytes, rest} <- take_bytes(rest, length) do
      {:ok, bytes, rest}
    end
  end

  defp decode_string(rest) do
    with {:ok, bytes, rest} <- decode_bytes(rest) do
      {:ok, bytes, rest}
    end
  end

  defp decode_reason(rest) do
    decode_string(rest)
  end

  defp decode_params(rest) do
    with {:ok, count, rest} <- MOQX.Varint.decode(rest) do
      decode_params_list(rest, count, [])
    end
  end

  defp decode_params_list(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_params_list(rest, count, acc) do
    with {:ok, kv, rest} <- decode_kv(rest) do
      decode_params_list(rest, count - 1, [kv | acc])
    end
  end

  defp decode_kv(rest) do
    with {:ok, type, rest} <- MOQX.Varint.decode(rest) do
      if rem(type, 2) == 0 do
        with {:ok, value, rest} <- MOQX.Varint.decode(rest) do
          {:ok, %MOQX.Common.KeyValuePair{type: type, kind: :varint, value: value}, rest}
        end
      else
        with {:ok, len, rest} <- MOQX.Varint.decode(rest),
             {:ok, bytes, rest} <- take_bytes(rest, len) do
          {:ok, %MOQX.Common.KeyValuePair{type: type, kind: :bytes, value: bytes}, rest}
        end
      end
    end
  end

  defp decode_varint_list(rest, 0), do: {:ok, [], rest}

  defp decode_varint_list(rest, count) do
    with {:ok, value, rest} <- MOQX.Varint.decode(rest),
         {:ok, tail, rest} <- decode_varint_list(rest, count - 1) do
      {:ok, [value | tail], rest}
    end
  end

  defp decode_subgroup_id(type, rest) do
    if MOQX.Data.subgroup_header_requires_explicit_id?(type) do
      case MOQX.Varint.decode(rest) do
        {:ok, subgroup_id, rest} -> {subgroup_id, rest}
        :need_more_data -> :need_more_data
      end
    else
      subgroup_id = if subgroup_type_subgroup_id_is_zero?(type), do: 0, else: nil
      {subgroup_id, rest}
    end
  end

  defp subgroup_type_has_extensions?(type) do
    type in [0x11, 0x13, 0x15, 0x19, 0x1B, 0x1D]
  end

  defp subgroup_type_subgroup_id_is_zero?(type) do
    type in [0x10, 0x11, 0x18, 0x19]
  end

  defp compute_object_id(nil, delta), do: delta
  defp compute_object_id(prev, delta), do: prev + delta + 1

  defp decode_object_extensions(rest, false), do: {:ok, nil, rest}

  defp decode_object_extensions(rest, true) do
    with {:ok, ext_len, rest} <- MOQX.Varint.decode(rest),
         {:ok, ext_bytes, rest} <- split_binary(rest, ext_len),
         {:ok, extensions} <- decode_extensions(ext_bytes, 0x01) do
      {:ok, extensions, rest}
    else
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_extensions(bytes, msg_type) do
    cond do
      msg_type == 0x00 and byte_size(bytes) != 0 ->
        {:error, :unexpected_extensions}

      byte_size(bytes) == 0 ->
        {:ok, nil}

      true ->
        decode_extensions_list(bytes, [])
    end
  end

  defp decode_extensions_list(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_extensions_list(rest, acc) do
    case decode_kv(rest) do
      {:ok, kv, rest} -> decode_extensions_list(rest, [kv | acc])
      :need_more_data -> :need_more_data
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_payload_or_status(rest, 0) do
    case MOQX.Varint.decode(rest) do
      {:ok, status_value, rest} ->
        {:ok, nil, decode_status(status_value), rest}

      :need_more_data ->
        :need_more_data
    end
  end

  defp decode_payload_or_status(rest, payload_len) do
    case split_binary(rest, payload_len) do
      {:ok, payload, rest} -> {:ok, payload, nil, rest}
      :need_more_data -> :need_more_data
    end
  end

  defp decode_status(0x0), do: :normal
  defp decode_status(0x1), do: :does_not_exist
  defp decode_status(0x3), do: :end_of_group
  defp decode_status(0x4), do: :end_of_track
  defp decode_status(_), do: :normal

  defp take_bytes(rest, 0), do: {:ok, <<>>, rest}

  defp take_bytes(rest, length) when is_integer(length) do
    split_binary(rest, length)
  end

  defp split_binary(rest, length) when is_integer(length) and length >= 0 do
    if byte_size(rest) < length do
      :need_more_data
    else
      <<bytes::binary-size(length), remaining::binary>> = rest
      {:ok, bytes, remaining}
    end
  end

  defp return_events_or_need_more(state, buffer, events) do
    if events == [] do
      {:need_more_data, %{state | buffer: buffer}}
    else
      {:ok, events, %{state | buffer: buffer}}
    end
  end
end
