defmodule MOQX.Naming do
  @moduledoc "Track naming structures."

  alias MOQX.Common.Tuple

  defmodule FullTrackName do
    @moduledoc "Namespace + track name."

    @type t :: %__MODULE__{namespace: Tuple.t(), name: binary()}

    defstruct namespace: %Tuple{}, name: <<>>
  end

  @type t :: FullTrackName.t()
end

defimpl MOQX.Marshaler, for: MOQX.Naming.FullTrackName do
  def marshal(%MOQX.Naming.FullTrackName{namespace: namespace, name: name}) do
    [
      MOQX.Marshaler.marshal(namespace),
      MOQX.Varint.encode(byte_size(name)),
      name
    ]
  end
end
