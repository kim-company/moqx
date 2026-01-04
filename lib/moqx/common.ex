defmodule MOQX.Common do
  @moduledoc """
  Common MOQT structures shared across control/data planes.
  """

  defmodule Tuple do
    @moduledoc "Namespace tuple (ordered list of namespace segments)."

    @type segment :: binary()
    @type t :: %__MODULE__{segments: [segment()]}

    defstruct segments: []
  end

  defmodule KeyValuePair do
    @moduledoc """
    Key-value pair for parameters and extensions.

    Even type values are varint, odd values are byte strings (Draft-14).
    """

    @type kind :: :varint | :bytes
    @type t :: %__MODULE__{type: non_neg_integer(), kind: kind(), value: non_neg_integer() | binary()}

    defstruct type: 0, kind: :varint, value: 0
  end

  defmodule Location do
    @moduledoc """
    Object location (group/object) as defined in Draft-14.
    """

    @type t :: %__MODULE__{group: non_neg_integer(), object: non_neg_integer()}

    defstruct group: 0, object: 0
  end

  @type t :: Tuple.t() | KeyValuePair.t() | Location.t()
end

defimpl MOQX.Marshaler, for: MOQX.Common.Tuple do
  def marshal(%MOQX.Common.Tuple{segments: segments}) do
    encoded_segments =
      Enum.map(segments, fn segment ->
        [MOQX.Varint.encode(byte_size(segment)), segment]
      end)

    [MOQX.Varint.encode(length(segments)), encoded_segments]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Common.KeyValuePair do
  def marshal(%MOQX.Common.KeyValuePair{type: type, kind: :varint, value: value}) do
    if rem(type, 2) != 0 do
      raise ArgumentError, "key-value type must be even for varint values"
    end

    [MOQX.Varint.encode(type), MOQX.Varint.encode(value)]
  end

  def marshal(%MOQX.Common.KeyValuePair{type: type, kind: :bytes, value: value}) do
    if rem(type, 2) == 0 do
      raise ArgumentError, "key-value type must be odd for byte values"
    end

    [MOQX.Varint.encode(type), MOQX.Varint.encode(byte_size(value)), value]
  end
end

defimpl MOQX.Marshaler, for: MOQX.Common.Location do
  def marshal(%MOQX.Common.Location{group: group, object: object}) do
    [MOQX.Varint.encode(group), MOQX.Varint.encode(object)]
  end
end
