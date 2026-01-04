defmodule MOQX.Relay do
  @moduledoc "Relay-side state structures."

  alias MOQX.Common.Location
  alias MOQX.Data.DatagramObject
  alias MOQX.Naming.FullTrackName

  defmodule TrackCache do
    @moduledoc "Cached objects for a track alias."

    @type t :: %__MODULE__{
            track_alias: non_neg_integer(),
            objects: %{Location.t() => DatagramObject.t()}
          }

    defstruct track_alias: 0, objects: %{}
  end

  defmodule Subscription do
    @moduledoc "Relay subscription state for a downstream subscriber."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            full_track_name: FullTrackName.t(),
            track_alias: non_neg_integer(),
            start_location: Location.t() | nil,
            end_group: non_neg_integer() | nil,
            forward: boolean(),
            subscriber_priority: non_neg_integer()
          }

    defstruct request_id: 0,
              full_track_name: %FullTrackName{},
              track_alias: 0,
              start_location: nil,
              end_group: nil,
              forward: false,
              subscriber_priority: 0
  end

  defmodule Fetch do
    @moduledoc "Relay fetch state for on-demand requests."

    @type t :: %__MODULE__{
            request_id: non_neg_integer(),
            track_alias: non_neg_integer(),
            start_location: Location.t(),
            end_location: Location.t()
          }

    defstruct request_id: 0,
              track_alias: 0,
              start_location: %Location{},
              end_location: %Location{}
  end

  @type t :: %__MODULE__{
          tracks: %{FullTrackName.t() => non_neg_integer()},
          track_aliases: %{non_neg_integer() => FullTrackName.t()},
          caches: %{non_neg_integer() => TrackCache.t()},
          subscriptions: %{non_neg_integer() => Subscription.t()},
          fetches: %{non_neg_integer() => Fetch.t()}
        }

  defstruct tracks: %{},
            track_aliases: %{},
            caches: %{},
            subscriptions: %{},
            fetches: %{}
end
