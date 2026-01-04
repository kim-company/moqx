# MOQX

MOQT (Media over QUIC Transport) protocol primitives for Elixir.

This library provides:
- Draft-14 data structures for control and data planes.
- `MOQX.Marshaler` protocol to serialize MOQT messages to iodata.
- `MOQX.Unmarshaler` state machine to decode control streams, data streams, and datagrams with partial input handling.
- QUIC varint encoder/decoder (`MOQX.Varint`).

The library is transport-agnostic: it produces/consumes binaries that can be
carried over QUIC streams or datagrams (e.g. via quichex).

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `moqx` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:moqx, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc).
