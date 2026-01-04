defmodule MOQX.VarintTest do
  use ExUnit.Case, async: true

  test "encodes varints" do
    assert MOQX.Varint.encode(0) == <<0b00000000>>
    assert MOQX.Varint.encode(1) == <<0b00000001>>
    assert MOQX.Varint.encode(63) == <<0b00111111>>
    assert MOQX.Varint.encode(127) == <<0b01000000, 127>>
    assert MOQX.Varint.encode(128) == <<0b01000000, 128>>
    assert MOQX.Varint.encode(300) == <<0b01000001, 44>>
    assert MOQX.Varint.encode(16_383) == <<0b01111111, 0xFF>>
    assert MOQX.Varint.encode(1_073_741_823) == <<0b10111111, 0xFF, 0xFF, 0xFF>>

    assert MOQX.Varint.encode(4_611_686_018_427_387_903) ==
             <<0b11111111, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
  end
end
