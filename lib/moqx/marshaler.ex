defprotocol MOQX.Marshaler do
  @moduledoc """
  Protocol defining how MOQT structs are marshaled to iodata.
  """

  @fallback_to_any true

  @spec marshal(t()) :: iodata()
  def marshal(value)
end

defimpl MOQX.Marshaler, for: List do
  def marshal(list), do: Enum.map(list, &MOQX.Marshaler.marshal/1)
end

defimpl MOQX.Marshaler, for: BitString do
  def marshal(binary), do: binary
end
