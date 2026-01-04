defmodule MOQX.Control do
  @moduledoc """
  MOQT control-plane messages (Draft-14 Section 9).
  """

  alias MOQX.Common.{KeyValuePair, Location, Tuple}

  @type group_order :: :original | :ascending | :descending
  @type filter_type :: :next_group_start | :latest_object | :absolute_start | :absolute_range
  @type fetch_type :: :standalone | :relative_fetch | :absolute_fetch

  defmodule ClientSetup do
    @moduledoc "CLIENT_SETUP message."

    @type t :: %__MODULE__{
            supported_versions: [non_neg_integer()],
            setup_parameters: [KeyValuePair.t()]
          }

    defstruct supported_versions: [], setup_parameters: []
  end

  defmodule ServerSetup do
    @moduledoc "SERVER_SETUP message."

    @type t :: %__MODULE__{
            selected_version: non_neg_integer(),
            setup_parameters: [KeyValuePair.t()]
          }

    defstruct selected_version: 0, setup_parameters: []
  end

  defmodule Goaway do
    @moduledoc "GOAWAY message."

    @type t :: %__MODULE__{new_session_uri: binary() | nil}

    defstruct new_session_uri: nil
  end

  defmodule MaxRequestId do
    @moduledoc "MAX_REQUEST_ID message."

    @type t :: %__MODULE__{max_request_id: non_neg_integer()}

    defstruct max_request_id: 0
  end

  defmodule RequestsBlocked do
    @moduledoc "REQUESTS_BLOCKED message."

    @type t :: %__MODULE__{max_request_id: non_neg_integer()}

    defstruct max_request_id: 0
  end

  defmodule Subscribe do
    @moduledoc "SUBSCRIBE message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_namespace: Tuple.t(),
            track_name: binary(),
            subscriber_priority: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            forward: boolean(),
            filter_type: MOQX.Control.filter_type(),
            start_location: Location.t() | nil,
            end_group: non_neg_integer() | nil,
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              track_namespace: %Tuple{},
              track_name: <<>>,
              subscriber_priority: 0,
              group_order: :ascending,
              forward: false,
              filter_type: :latest_object,
              start_location: nil,
              end_group: nil,
              parameters: []
  end

  defmodule SubscribeOk do
    @moduledoc "SUBSCRIBE_OK message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_alias: non_neg_integer(),
            expires: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            content_exists: boolean(),
            largest_location: Location.t() | nil,
            parameters: [KeyValuePair.t()] | nil
          }

    defstruct request_id: 0,
              track_alias: 0,
              expires: 0,
              group_order: :ascending,
              content_exists: false,
              largest_location: nil,
              parameters: nil
  end

  defmodule SubscribeError do
    @moduledoc "SUBSCRIBE_ERROR message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            error_code: non_neg_integer(),
            reason_phrase: binary()
          }

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule SubscribeUpdate do
    @moduledoc "SUBSCRIBE_UPDATE message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            subscription_request_id: non_neg_integer(),
            start_location: Location.t(),
            end_group: non_neg_integer(),
            subscriber_priority: non_neg_integer(),
            forward: boolean(),
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              subscription_request_id: 0,
              start_location: %Location{},
              end_group: 0,
              subscriber_priority: 0,
              forward: false,
              parameters: []
  end

  defmodule Unsubscribe do
    @moduledoc "UNSUBSCRIBE message."

    @type t :: %__MODULE__{request_id: non_neg_integer()}

    defstruct request_id: 0
  end

  defmodule FetchStandaloneProps do
    @moduledoc "Standalone FETCH properties."

    @type t :: %__MODULE__{
            track_namespace: Tuple.t(),
            track_name: binary(),
            start_location: Location.t(),
            end_location: Location.t()
          }

    defstruct track_namespace: %Tuple{},
              track_name: <<>>,
              start_location: %Location{},
              end_location: %Location{}
  end

  defmodule FetchJoiningProps do
    @moduledoc "Joining FETCH properties."

    @type t :: %__MODULE__{joining_request_id: non_neg_integer(), joining_start: non_neg_integer()}

    defstruct joining_request_id: 0, joining_start: 0
  end

  defmodule Fetch do
    @moduledoc "FETCH message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            subscriber_priority: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            fetch_type: MOQX.Control.fetch_type(),
            standalone: FetchStandaloneProps.t() | nil,
            joining: FetchJoiningProps.t() | nil,
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              subscriber_priority: 0,
              group_order: :ascending,
              fetch_type: :standalone,
              standalone: nil,
              joining: nil,
              parameters: []
  end

  defmodule FetchOk do
    @moduledoc "FETCH_OK message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            end_of_track: boolean(),
            end_location: Location.t(),
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              group_order: :ascending,
              end_of_track: false,
              end_location: %Location{},
              parameters: []
  end

  defmodule FetchError do
    @moduledoc "FETCH_ERROR message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            error_code: non_neg_integer(),
            reason_phrase: binary()
          }

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule FetchCancel do
    @moduledoc "FETCH_CANCEL message."

    @type t :: %__MODULE__{request_id: non_neg_integer()}

    defstruct request_id: 0
  end

  defmodule TrackStatus do
    @moduledoc "TRACK_STATUS message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_namespace: Tuple.t(),
            track_name: binary(),
            subscriber_priority: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            forward: boolean(),
            filter_type: MOQX.Control.filter_type(),
            start_location: Location.t() | nil,
            end_group: non_neg_integer() | nil,
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              track_namespace: %Tuple{},
              track_name: <<>>,
              subscriber_priority: 0,
              group_order: :ascending,
              forward: false,
              filter_type: :latest_object,
              start_location: nil,
              end_group: nil,
              parameters: []
  end

  defmodule TrackStatusOk do
    @moduledoc "TRACK_STATUS_OK message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_alias: non_neg_integer(),
            expires: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            content_exists: boolean(),
            largest_location: Location.t() | nil,
            parameters: [KeyValuePair.t()] | nil
          }

    defstruct request_id: 0,
              track_alias: 0,
              expires: 0,
              group_order: :ascending,
              content_exists: false,
              largest_location: nil,
              parameters: nil
  end

  defmodule TrackStatusError do
    @moduledoc "TRACK_STATUS_ERROR message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            error_code: non_neg_integer(),
            reason_phrase: binary()
          }

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule PublishNamespace do
    @moduledoc "PUBLISH_NAMESPACE message."

    @type t :: %__MODULE__{request_id: non_neg_integer(), track_namespace: Tuple.t(), parameters: [KeyValuePair.t()]}

    defstruct request_id: 0, track_namespace: %Tuple{}, parameters: []
  end

  defmodule PublishNamespaceOk do
    @moduledoc "PUBLISH_NAMESPACE_OK message."

    @type t :: %__MODULE__{request_id: non_neg_integer()}

    defstruct request_id: 0
  end

  defmodule PublishNamespaceError do
    @moduledoc "PUBLISH_NAMESPACE_ERROR message."

    @type t :: %__MODULE__{request_id: non_neg_integer(), error_code: non_neg_integer(), reason_phrase: binary()}

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule PublishNamespaceDone do
    @moduledoc "PUBLISH_NAMESPACE_DONE message."

    @type t :: %__MODULE__{track_namespace: Tuple.t()}

    defstruct track_namespace: %Tuple{}
  end

  defmodule PublishNamespaceCancel do
    @moduledoc "PUBLISH_NAMESPACE_CANCEL message."

    @type t :: %__MODULE__{track_namespace: Tuple.t(), error_code: non_neg_integer(), reason_phrase: binary()}

    defstruct track_namespace: %Tuple{}, error_code: 0, reason_phrase: <<>>
  end

  defmodule SubscribeNamespace do
    @moduledoc "SUBSCRIBE_NAMESPACE message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_namespace_prefix: Tuple.t(),
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0, track_namespace_prefix: %Tuple{}, parameters: []
  end

  defmodule SubscribeNamespaceOk do
    @moduledoc "SUBSCRIBE_NAMESPACE_OK message."

    @type t :: %__MODULE__{request_id: non_neg_integer()}

    defstruct request_id: 0
  end

  defmodule SubscribeNamespaceError do
    @moduledoc "SUBSCRIBE_NAMESPACE_ERROR message."

    @type t :: %__MODULE__{request_id: non_neg_integer(), error_code: non_neg_integer(), reason_phrase: binary()}

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule UnsubscribeNamespace do
    @moduledoc "UNSUBSCRIBE_NAMESPACE message."

    @type t :: %__MODULE__{track_namespace_prefix: Tuple.t()}

    defstruct track_namespace_prefix: %Tuple{}
  end

  defmodule Publish do
    @moduledoc "PUBLISH message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_namespace: Tuple.t(),
            track_name: binary(),
            track_alias: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            content_exists: boolean(),
            largest_location: Location.t() | nil,
            forward: boolean(),
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              track_namespace: %Tuple{},
              track_name: <<>>,
              track_alias: 0,
              group_order: :ascending,
              content_exists: false,
              largest_location: nil,
              forward: false,
              parameters: []
  end

  defmodule PublishOk do
    @moduledoc "PUBLISH_OK message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            forward: boolean(),
            subscriber_priority: non_neg_integer(),
            group_order: MOQX.Control.group_order(),
            filter_type: MOQX.Control.filter_type(),
            start_location: Location.t() | nil,
            end_group: non_neg_integer() | nil,
            parameters: [KeyValuePair.t()]
          }

    defstruct request_id: 0,
              forward: false,
              subscriber_priority: 0,
              group_order: :ascending,
              filter_type: :latest_object,
              start_location: nil,
              end_group: nil,
              parameters: []
  end

  defmodule PublishError do
    @moduledoc "PUBLISH_ERROR message."

    @type t :: %__MODULE__{request_id: non_neg_integer(), error_code: non_neg_integer(), reason_phrase: binary()}

    defstruct request_id: 0, error_code: 0, reason_phrase: <<>>
  end

  defmodule PublishDone do
    @moduledoc "PUBLISH_DONE message."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            status_code: non_neg_integer(),
            stream_count: non_neg_integer(),
            reason_phrase: binary()
          }

    defstruct request_id: 0, status_code: 0, stream_count: 0, reason_phrase: <<>>
  end

  @type t ::
          ClientSetup.t()
          | ServerSetup.t()
          | Goaway.t()
          | MaxRequestId.t()
          | RequestsBlocked.t()
          | Subscribe.t()
          | SubscribeOk.t()
          | SubscribeError.t()
          | SubscribeUpdate.t()
          | Unsubscribe.t()
          | Fetch.t()
          | FetchOk.t()
          | FetchError.t()
          | FetchCancel.t()
          | TrackStatus.t()
          | TrackStatusOk.t()
          | TrackStatusError.t()
          | PublishNamespace.t()
          | PublishNamespaceOk.t()
          | PublishNamespaceError.t()
          | PublishNamespaceDone.t()
          | PublishNamespaceCancel.t()
          | SubscribeNamespace.t()
          | SubscribeNamespaceOk.t()
          | SubscribeNamespaceError.t()
          | UnsubscribeNamespace.t()
          | Publish.t()
          | PublishOk.t()
          | PublishError.t()
          | PublishDone.t()

  @doc false
  def encode_control_frame(type, payload) do
    payload_bin = IO.iodata_to_binary(payload)
    length = byte_size(payload_bin)

    if length > 0xFFFF do
      raise ArgumentError, "control payload length exceeds 65535"
    end

    [MOQX.Varint.encode(type), <<length::16>>, payload_bin]
  end

  @doc false
  def group_order_value(:original), do: 0x0
  def group_order_value(:ascending), do: 0x1
  def group_order_value(:descending), do: 0x2

  @doc false
  def filter_type_value(:next_group_start), do: 0x1
  def filter_type_value(:latest_object), do: 0x2
  def filter_type_value(:absolute_start), do: 0x3
  def filter_type_value(:absolute_range), do: 0x4

  @doc false
  def fetch_type_value(:standalone), do: 0x1
  def fetch_type_value(:relative_fetch), do: 0x2
  def fetch_type_value(:absolute_fetch), do: 0x3

  @doc false
  def encode_bool(true), do: 1
  def encode_bool(false), do: 0

  @doc false
  def encode_reason(reason) when is_binary(reason) do
    [MOQX.Varint.encode(byte_size(reason)), reason]
  end

  @doc false
  def encode_params(nil), do: MOQX.Varint.encode(0)
  def encode_params(params) when is_list(params) do
    [MOQX.Varint.encode(length(params)), Enum.map(params, &MOQX.Marshaler.marshal/1)]
  end

  @doc false
  def encode_location_or_default(nil), do: MOQX.Marshaler.marshal(%Location{group: 0, object: 0})
  def encode_location_or_default(location), do: MOQX.Marshaler.marshal(location)
end


defimpl MOQX.Marshaler, for: MOQX.Control.ClientSetup do
  def marshal(%MOQX.Control.ClientSetup{} = msg) do
    payload = [
      MOQX.Varint.encode(length(msg.supported_versions)),
      Enum.map(msg.supported_versions, &MOQX.Varint.encode/1),
      MOQX.Control.encode_params(msg.setup_parameters)
    ]

    MOQX.Control.encode_control_frame(0x20, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.ServerSetup do
  def marshal(%MOQX.Control.ServerSetup{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.selected_version),
      MOQX.Control.encode_params(msg.setup_parameters)
    ]

    MOQX.Control.encode_control_frame(0x21, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.Goaway do
  def marshal(%MOQX.Control.Goaway{new_session_uri: nil}) do
    MOQX.Control.encode_control_frame(0x10, MOQX.Varint.encode(0))
  end

  def marshal(%MOQX.Control.Goaway{new_session_uri: uri}) do
    payload = [MOQX.Varint.encode(byte_size(uri)), uri]
    MOQX.Control.encode_control_frame(0x10, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.MaxRequestId do
  def marshal(%MOQX.Control.MaxRequestId{max_request_id: max_request_id}) do
    MOQX.Control.encode_control_frame(0x15, MOQX.Varint.encode(max_request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.RequestsBlocked do
  def marshal(%MOQX.Control.RequestsBlocked{max_request_id: max_request_id}) do
    MOQX.Control.encode_control_frame(0x1A, MOQX.Varint.encode(max_request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.Subscribe do
  def marshal(%MOQX.Control.Subscribe{} = msg) do
    filter_type = MOQX.Control.filter_type_value(msg.filter_type)

    range_fields =
      case msg.filter_type do
        :absolute_start ->
          [
            require_location(msg.start_location),
            []
          ]

        :absolute_range ->
          [
            require_location(msg.start_location),
            require_end_group(msg.end_group)
          ]

        _ ->
          []
      end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Marshaler.marshal(msg.track_namespace),
      MOQX.Varint.encode(byte_size(msg.track_name)),
      msg.track_name,
      <<msg.subscriber_priority::8>>,
      <<MOQX.Control.group_order_value(msg.group_order)::8>>,
      <<MOQX.Control.encode_bool(msg.forward)::8>>,
      MOQX.Varint.encode(filter_type),
      range_fields,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x03, payload)
  end

  defp require_location(nil), do: raise(ArgumentError, "start_location required for filter type")
  defp require_location(location), do: MOQX.Marshaler.marshal(location)
  defp require_end_group(nil), do: raise(ArgumentError, "end_group required for filter type")
  defp require_end_group(end_group), do: MOQX.Varint.encode(end_group)
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeOk do
  def marshal(%MOQX.Control.SubscribeOk{} = msg) do
    group_order = MOQX.Control.group_order_value(msg.group_order)

    if group_order == 0x0 do
      raise ArgumentError, "group_order must be ascending or descending"
    end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.track_alias),
      MOQX.Varint.encode(msg.expires),
      <<group_order::8>>,
      <<MOQX.Control.encode_bool(msg.content_exists)::8>>,
      if(msg.content_exists, do: MOQX.Control.encode_location_or_default(msg.largest_location), else: []),
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x04, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeError do
  def marshal(%MOQX.Control.SubscribeError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x05, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeUpdate do
  def marshal(%MOQX.Control.SubscribeUpdate{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.subscription_request_id),
      MOQX.Marshaler.marshal(msg.start_location),
      MOQX.Varint.encode(msg.end_group),
      <<msg.subscriber_priority::8>>,
      <<MOQX.Control.encode_bool(msg.forward)::8>>,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x02, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.Unsubscribe do
  def marshal(%MOQX.Control.Unsubscribe{request_id: request_id}) do
    MOQX.Control.encode_control_frame(0x0A, MOQX.Varint.encode(request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.Fetch do
  def marshal(%MOQX.Control.Fetch{} = msg) do
    fetch_type = MOQX.Control.fetch_type_value(msg.fetch_type)

    fetch_fields =
      case msg.fetch_type do
        :standalone ->
          if is_nil(msg.standalone) do
            raise ArgumentError, "standalone properties required for standalone fetch"
          end

          [
            MOQX.Marshaler.marshal(msg.standalone.track_namespace),
            MOQX.Varint.encode(byte_size(msg.standalone.track_name)),
            msg.standalone.track_name,
            MOQX.Marshaler.marshal(msg.standalone.start_location),
            MOQX.Marshaler.marshal(msg.standalone.end_location)
          ]

        :relative_fetch ->
          joining = require_joining(msg.joining)
          [MOQX.Varint.encode(joining.joining_request_id), MOQX.Varint.encode(joining.joining_start)]

        :absolute_fetch ->
          joining = require_joining(msg.joining)
          [MOQX.Varint.encode(joining.joining_request_id), MOQX.Varint.encode(joining.joining_start)]
      end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      <<msg.subscriber_priority::8>>,
      <<MOQX.Control.group_order_value(msg.group_order)::8>>,
      MOQX.Varint.encode(fetch_type),
      fetch_fields,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x16, payload)
  end

  defp require_joining(nil), do: raise(ArgumentError, "joining properties required for joining fetch")
  defp require_joining(joining), do: joining
end


defimpl MOQX.Marshaler, for: MOQX.Control.FetchOk do
  def marshal(%MOQX.Control.FetchOk{} = msg) do
    group_order = MOQX.Control.group_order_value(msg.group_order)

    if group_order == 0x0 do
      raise ArgumentError, "group_order must be ascending or descending"
    end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      <<group_order::8>>,
      <<MOQX.Control.encode_bool(msg.end_of_track)::8>>,
      MOQX.Marshaler.marshal(msg.end_location),
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x18, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.FetchError do
  def marshal(%MOQX.Control.FetchError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x19, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.FetchCancel do
  def marshal(%MOQX.Control.FetchCancel{request_id: request_id}) do
    MOQX.Control.encode_control_frame(0x17, MOQX.Varint.encode(request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.TrackStatus do
  def marshal(%MOQX.Control.TrackStatus{} = msg) do
    filter_type = MOQX.Control.filter_type_value(msg.filter_type)

    range_fields =
      case msg.filter_type do
        :absolute_start ->
          [
            require_location(msg.start_location),
            []
          ]

        :absolute_range ->
          [
            require_location(msg.start_location),
            require_end_group(msg.end_group)
          ]

        _ ->
          []
      end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Marshaler.marshal(msg.track_namespace),
      MOQX.Varint.encode(byte_size(msg.track_name)),
      msg.track_name,
      <<msg.subscriber_priority::8>>,
      <<MOQX.Control.group_order_value(msg.group_order)::8>>,
      <<MOQX.Control.encode_bool(msg.forward)::8>>,
      MOQX.Varint.encode(filter_type),
      range_fields,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x0D, payload)
  end

  defp require_location(nil), do: raise(ArgumentError, "start_location required for filter type")
  defp require_location(location), do: MOQX.Marshaler.marshal(location)
  defp require_end_group(nil), do: raise(ArgumentError, "end_group required for filter type")
  defp require_end_group(end_group), do: MOQX.Varint.encode(end_group)
end


defimpl MOQX.Marshaler, for: MOQX.Control.TrackStatusOk do
  def marshal(%MOQX.Control.TrackStatusOk{} = msg) do
    group_order = MOQX.Control.group_order_value(msg.group_order)

    if group_order == 0x0 do
      raise ArgumentError, "group_order must be ascending or descending"
    end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.track_alias),
      MOQX.Varint.encode(msg.expires),
      <<group_order::8>>,
      <<MOQX.Control.encode_bool(msg.content_exists)::8>>,
      if(msg.content_exists, do: MOQX.Control.encode_location_or_default(msg.largest_location), else: []),
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x0E, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.TrackStatusError do
  def marshal(%MOQX.Control.TrackStatusError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x0F, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishNamespace do
  def marshal(%MOQX.Control.PublishNamespace{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Marshaler.marshal(msg.track_namespace),
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x06, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishNamespaceOk do
  def marshal(%MOQX.Control.PublishNamespaceOk{request_id: request_id}) do
    MOQX.Control.encode_control_frame(0x07, MOQX.Varint.encode(request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishNamespaceError do
  def marshal(%MOQX.Control.PublishNamespaceError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x08, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishNamespaceDone do
  def marshal(%MOQX.Control.PublishNamespaceDone{} = msg) do
    payload = MOQX.Marshaler.marshal(msg.track_namespace)
    MOQX.Control.encode_control_frame(0x09, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishNamespaceCancel do
  def marshal(%MOQX.Control.PublishNamespaceCancel{} = msg) do
    payload = [
      MOQX.Marshaler.marshal(msg.track_namespace),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x0C, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeNamespace do
  def marshal(%MOQX.Control.SubscribeNamespace{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Marshaler.marshal(msg.track_namespace_prefix),
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x11, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeNamespaceOk do
  def marshal(%MOQX.Control.SubscribeNamespaceOk{request_id: request_id}) do
    MOQX.Control.encode_control_frame(0x12, MOQX.Varint.encode(request_id))
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.SubscribeNamespaceError do
  def marshal(%MOQX.Control.SubscribeNamespaceError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x13, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.UnsubscribeNamespace do
  def marshal(%MOQX.Control.UnsubscribeNamespace{} = msg) do
    payload = MOQX.Marshaler.marshal(msg.track_namespace_prefix)
    MOQX.Control.encode_control_frame(0x14, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.Publish do
  def marshal(%MOQX.Control.Publish{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Marshaler.marshal(msg.track_namespace),
      MOQX.Varint.encode(byte_size(msg.track_name)),
      msg.track_name,
      MOQX.Varint.encode(msg.track_alias),
      <<MOQX.Control.group_order_value(msg.group_order)::8>>,
      <<MOQX.Control.encode_bool(msg.content_exists)::8>>,
      if(msg.content_exists, do: require_location(msg.largest_location), else: []),
      <<MOQX.Control.encode_bool(msg.forward)::8>>,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x1D, payload)
  end

  defp require_location(nil), do: raise(ArgumentError, "largest_location required when content_exists is true")
  defp require_location(location), do: MOQX.Marshaler.marshal(location)
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishOk do
  def marshal(%MOQX.Control.PublishOk{} = msg) do
    filter_type = MOQX.Control.filter_type_value(msg.filter_type)

    range_fields =
      case msg.filter_type do
        :absolute_start ->
          [
            require_location(msg.start_location),
            []
          ]

        :absolute_range ->
          [
            require_location(msg.start_location),
            require_end_group(msg.end_group)
          ]

        _ ->
          []
      end

    payload = [
      MOQX.Varint.encode(msg.request_id),
      <<MOQX.Control.encode_bool(msg.forward)::8>>,
      <<msg.subscriber_priority::8>>,
      <<MOQX.Control.group_order_value(msg.group_order)::8>>,
      MOQX.Varint.encode(filter_type),
      range_fields,
      MOQX.Control.encode_params(msg.parameters)
    ]

    MOQX.Control.encode_control_frame(0x1E, payload)
  end

  defp require_location(nil), do: raise(ArgumentError, "start_location required for filter type")
  defp require_location(location), do: MOQX.Marshaler.marshal(location)
  defp require_end_group(nil), do: raise(ArgumentError, "end_group required for filter type")
  defp require_end_group(end_group), do: MOQX.Varint.encode(end_group)
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishError do
  def marshal(%MOQX.Control.PublishError{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.error_code),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x1F, payload)
  end
end


defimpl MOQX.Marshaler, for: MOQX.Control.PublishDone do
  def marshal(%MOQX.Control.PublishDone{} = msg) do
    payload = [
      MOQX.Varint.encode(msg.request_id),
      MOQX.Varint.encode(msg.status_code),
      MOQX.Varint.encode(msg.stream_count),
      MOQX.Control.encode_reason(msg.reason_phrase)
    ]

    MOQX.Control.encode_control_frame(0x0B, payload)
  end
end
