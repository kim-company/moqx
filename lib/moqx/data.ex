defmodule MOQX.Data do
  @moduledoc """
  MOQT data plane structures (objects + headers).
  """

  alias MOQX.Common.KeyValuePair

  defmodule ObjectStatus do
    @moduledoc """
    Object status codes as atoms (Draft-14 Section 10.2).
    """

    @type t :: :normal | :does_not_exist | :end_of_group | :end_of_track
  end

  defmodule DatagramObject do
    @moduledoc "Object datagram message (Draft-14 Section 10.3)."

    @type t :: %__MODULE__{
            track_alias: non_neg_integer(),
            group_id: non_neg_integer(),
            object_id: non_neg_integer(),
            publisher_priority: non_neg_integer(),
            extension_headers: [KeyValuePair.t()] | nil,
            payload: binary()
          }

    defstruct track_alias: 0,
              group_id: 0,
              object_id: 0,
              publisher_priority: 0,
              extension_headers: nil,
              payload: <<>>
  end

  defmodule SubgroupHeader do
    @moduledoc "Subgroup stream header (Draft-14 Section 10.4.2)."

    @type header_type :: 0x10 | 0x11 | 0x12 | 0x13 | 0x14 | 0x15 | 0x18 | 0x19 | 0x1A | 0x1B | 0x1C | 0x1D

    @type t :: %__MODULE__{
            header_type: header_type(),
            track_alias: non_neg_integer(),
            group_id: non_neg_integer(),
            subgroup_id: non_neg_integer() | nil,
            publisher_priority: non_neg_integer()
          }

    defstruct header_type: 0x10,
              track_alias: 0,
              group_id: 0,
              subgroup_id: nil,
              publisher_priority: 0
  end

  defmodule SubgroupObject do
    @moduledoc """
    Object inside a subgroup stream (Draft-14 Section 10.4.2).

    `previous_object_id` and `has_extensions` are required for encoding.
    """

    @type t :: %__MODULE__{
            object_id: non_neg_integer(),
            previous_object_id: non_neg_integer() | nil,
            has_extensions: boolean(),
            extension_headers: [KeyValuePair.t()] | nil,
            object_status: ObjectStatus.t() | nil,
            payload: binary() | nil
          }

    defstruct object_id: 0,
              previous_object_id: nil,
              has_extensions: false,
              extension_headers: nil,
              object_status: nil,
              payload: nil
  end

  defmodule FetchHeader do
    @moduledoc "Fetch stream header (Draft-14 Section 10.4.4)."

    @type t :: %__MODULE__{request_id: non_neg_integer()}

    defstruct request_id: 0
  end

  defmodule FetchObject do
    @moduledoc "Fetch object (Draft-14 Section 10.4.4)."

    @type t :: %__MODULE__{
            group_id: non_neg_integer(),
            subgroup_id: non_neg_integer(),
            object_id: non_neg_integer(),
            publisher_priority: non_neg_integer(),
            extension_headers: [KeyValuePair.t()] | nil,
            object_status: ObjectStatus.t() | nil,
            payload: binary() | nil
          }

    defstruct group_id: 0,
              subgroup_id: 0,
              object_id: 0,
              publisher_priority: 0,
              extension_headers: nil,
              object_status: nil,
              payload: nil
  end

  @type t ::
          DatagramObject.t()
          | SubgroupHeader.t()
          | SubgroupObject.t()
          | FetchHeader.t()
          | FetchObject.t()

  @doc false
  @spec object_status_value(ObjectStatus.t()) :: non_neg_integer()
  def object_status_value(:normal), do: 0x0
  def object_status_value(:does_not_exist), do: 0x1
  def object_status_value(:end_of_group), do: 0x3
  def object_status_value(:end_of_track), do: 0x4

  @doc false
  @spec subgroup_header_requires_explicit_id?(SubgroupHeader.header_type()) :: boolean()
  def subgroup_header_requires_explicit_id?(type) when type in [0x14, 0x15, 0x1C, 0x1D], do: true
  def subgroup_header_requires_explicit_id?(_type), do: false
end

defimpl MOQX.Marshaler, for: MOQX.Data.DatagramObject do
  def marshal(%MOQX.Data.DatagramObject{} = object) do
    has_extensions = object.extension_headers != nil
    type = if has_extensions, do: 0x01, else: 0x00

    ext_payload =
      object.extension_headers
      |> List.wrap()
      |> Enum.map(&MOQX.Marshaler.marshal/1)
      |> IO.iodata_to_binary()

    [
      MOQX.Varint.encode(type),
      MOQX.Varint.encode(object.track_alias),
      MOQX.Varint.encode(object.group_id),
      MOQX.Varint.encode(object.object_id),
      <<object.publisher_priority::8>>,
      MOQX.Varint.encode(byte_size(ext_payload)),
      ext_payload,
      object.payload
    ]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Data.SubgroupHeader do
  def marshal(%MOQX.Data.SubgroupHeader{} = header) do
    header_fields = [
      MOQX.Varint.encode(header.header_type),
      MOQX.Varint.encode(header.track_alias),
      MOQX.Varint.encode(header.group_id)
    ]

    subgroup_field =
      if MOQX.Data.subgroup_header_requires_explicit_id?(header.header_type) do
        if is_nil(header.subgroup_id) do
          raise ArgumentError, "subgroup_id required for header_type #{header.header_type}"
        end

        MOQX.Varint.encode(header.subgroup_id)
      else
        []
      end

    [header_fields, subgroup_field, <<header.publisher_priority::8>>]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Data.SubgroupObject do
  def marshal(%MOQX.Data.SubgroupObject{} = object) do
    delta =
      case object.previous_object_id do
        nil -> object.object_id
        prev when prev < object.object_id -> object.object_id - prev - 1
        _ -> raise ArgumentError, "previous_object_id must be smaller than object_id"
      end

    extension_payload =
      if object.has_extensions do
        object.extension_headers
        |> List.wrap()
        |> Enum.map(&MOQX.Marshaler.marshal/1)
        |> IO.iodata_to_binary()
      else
        nil
      end

    payload_section =
      if is_binary(object.payload) and byte_size(object.payload) > 0 do
        [MOQX.Varint.encode(byte_size(object.payload)), object.payload]
      else
        status = object.object_status || :normal
        [MOQX.Varint.encode(0), MOQX.Varint.encode(MOQX.Data.object_status_value(status))]
      end

    [
      MOQX.Varint.encode(delta),
      if(object.has_extensions,
        do: [MOQX.Varint.encode(byte_size(extension_payload || <<>>)), extension_payload],
        else: []
      ),
      payload_section
    ]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Data.FetchHeader do
  def marshal(%MOQX.Data.FetchHeader{request_id: request_id}) do
    [MOQX.Varint.encode(0x05), MOQX.Varint.encode(request_id)]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Data.FetchObject do
  def marshal(%MOQX.Data.FetchObject{} = object) do
    extension_payload =
      object.extension_headers
      |> List.wrap()
      |> Enum.map(&MOQX.Marshaler.marshal/1)
      |> IO.iodata_to_binary()

    payload_section =
      if is_binary(object.payload) and byte_size(object.payload) > 0 do
        [MOQX.Varint.encode(byte_size(object.payload)), object.payload]
      else
        status = object.object_status || :normal
        [MOQX.Varint.encode(0), MOQX.Varint.encode(MOQX.Data.object_status_value(status))]
      end

    [
      MOQX.Varint.encode(object.group_id),
      MOQX.Varint.encode(object.subgroup_id),
      MOQX.Varint.encode(object.object_id),
      <<object.publisher_priority::8>>,
      MOQX.Varint.encode(byte_size(extension_payload)),
      extension_payload,
      payload_section
    ]
  end
end
