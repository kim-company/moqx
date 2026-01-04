defmodule MOQX.Session do
  @moduledoc "Session-level state for a MOQT connection."

  alias MOQX.Common.KeyValuePair

  @type role :: :client | :server

  @type t :: %__MODULE__{
          role: role(),
          version: non_neg_integer(),
          parameters: [KeyValuePair.t()],
          max_request_id: non_neg_integer(),
          control_stream_id: non_neg_integer() | nil
        }

  defstruct role: :client,
            version: 0,
            parameters: [],
            max_request_id: 0,
            control_stream_id: nil
end
