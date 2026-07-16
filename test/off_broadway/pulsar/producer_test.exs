defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer

  test "invokes the MFA callback with metadata followed by the configured arguments" do
    callback = {__MODULE__, :notify_active_state, [self(), :configured_argument]}

    consumer_pid = spawn(fn -> :ok end)
    state = %{subscription: "my-subscription", active_state_callback: callback}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :active, consumer_pid, "my-topic"},
             state
           ) == {:noreply, [], state}

    assert_receive {:active_state_callback,
                    %{
                      active_state: :active,
                      topic: "my-topic",
                      subscription: "my-subscription",
                      consumer_pid: ^consumer_pid
                    }, :configured_argument}
  end

  test "handles active state changes when no callback is configured" do
    state = %{subscription: "my-subscription", active_state_callback: nil}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :passive, self(), "my-topic"},
             state
           ) == {:noreply, [], state}
  end

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

  def notify_active_state(metadata, test_pid, configured_argument) do
    send(test_pid, {:active_state_callback, metadata, configured_argument})
  end
end
