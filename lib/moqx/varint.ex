defmodule MOQX.Varint do
  @moduledoc """
  QUIC varint encoding helpers (RFC 9000).
  """

  import Bitwise

  @max_value (1 <<< 62) - 1

  @spec encode(non_neg_integer()) :: binary()
  def encode(value) when is_integer(value) and value >= 0 and value <= @max_value do
    cond do
      value < (1 <<< 6) ->
        <<value::8>>

      value < (1 <<< 14) ->
        <<0b01::2, value::14>>

      value < (1 <<< 30) ->
        <<0b10::2, value::30>>

      true ->
        <<0b11::2, value::62>>
    end
  end

  def encode(value) when is_integer(value) do
    raise ArgumentError, "varint overflow: #{value}"
  end

  @spec decode(binary()) :: {:ok, non_neg_integer(), binary()} | :need_more_data | {:error, :invalid_varint}
  def decode(<<>>), do: :need_more_data

  def decode(<<first::8, rest::binary>>) do
    prefix = first >>> 6
    len =
      case prefix do
        0 -> 1
        1 -> 2
        2 -> 4
        3 -> 8
      end

    if byte_size(rest) < len - 1 do
      :need_more_data
    else
      {value, remaining} = decode_with_length(len, first, rest)
      {:ok, value, remaining}
    end
  end

  defp decode_with_length(1, first, rest) do
    value = first &&& 0b0011_1111
    {value, rest}
  end

  defp decode_with_length(2, first, rest) do
    <<b2::8, remaining::binary>> = rest
    value = ((first &&& 0b0011_1111) <<< 8) ||| b2
    {value, remaining}
  end

  defp decode_with_length(4, first, rest) do
    <<b2::8, b3::8, b4::8, remaining::binary>> = rest
    value =
      ((first &&& 0b0011_1111) <<< 24) |||
        (b2 <<< 16) |||
        (b3 <<< 8) |||
        b4

    {value, remaining}
  end

  defp decode_with_length(8, first, rest) do
    <<b2::8, b3::8, b4::8, b5::8, b6::8, b7::8, b8::8, remaining::binary>> = rest

    value =
      ((first &&& 0b0011_1111) <<< 56) |||
        (b2 <<< 48) |||
        (b3 <<< 40) |||
        (b4 <<< 32) |||
        (b5 <<< 24) |||
        (b6 <<< 16) |||
        (b7 <<< 8) |||
        b8

    {value, remaining}
  end
end
