defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer

  test "rejects an active state callback that is not an MFA tuple" do
    assert_raise ArgumentError, ~r/expected :active_state_callback to be a \{module, function, extra_args\} tuple/, fn ->
      Producer.init(
        topic: "my-topic",
        subscription: "my-subscription",
        active_state_callback: fn _metadata -> :ok end
      )
    end
  end

  test "rejects an MFA callback when the function is not exported at the effective arity" do
    assert_raise ArgumentError,
                 ~r/expected :active_state_callback .*\.notify_active_stat\/1 to be exported/,
                 fn ->
                   Producer.init(
                     topic: "my-topic",
                     subscription: "my-subscription",
                     active_state_callback: {__MODULE__, :notify_active_stat, []}
                   )
                 end

    assert_raise ArgumentError,
                 ~r/expected :active_state_callback .*\.notify_active_state\/2 to be exported/,
                 fn ->
                   Producer.init(
                     topic: "my-topic",
                     subscription: "my-subscription",
                     active_state_callback: {__MODULE__, :notify_active_state, [:one_extra_argument]}
                   )
                 end
  end

  def notify_active_state(_metadata, _configured_argument, _another_argument), do: :ok
end
