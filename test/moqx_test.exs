defmodule MOQXTest do
  use ExUnit.Case
  doctest MOQX

  test "greets the world" do
    assert MOQX.hello() == :world
  end
end
