defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer

  @event [:off_broadway_pulsar, :consumer, :active_state_changed]

  test "emits telemetry and invokes the callback when active state changes" do
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@event])
    test_pid = self()
    callback = fn metadata -> send(test_pid, {:active_state_callback, metadata}) end

    consumer_pid = spawn(fn -> :ok end)
    state = %{subscription: "my-subscription", active_state_callback: callback}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :active, consumer_pid, "my-topic"},
             state
           ) == {:noreply, [], state}

    assert_receive {@event, ^telemetry_ref, %{system_time: system_time}, metadata}
    assert is_integer(system_time)

    assert metadata == %{
             active_state: :active,
             topic: "my-topic",
             subscription: "my-subscription",
             consumer_pid: consumer_pid
           }

    assert_receive {:active_state_callback, ^metadata}
  end

  test "emits telemetry when no callback is configured" do
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@event])
    state = %{subscription: "my-subscription", active_state_callback: nil}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :passive, self(), "my-topic"},
             state
           ) == {:noreply, [], state}

    assert_receive {@event, ^telemetry_ref, _measurements, %{active_state: :passive}}
  end

  test "rejects an active state callback with the wrong arity" do
    assert_raise ArgumentError, ~r/expected :active_state_callback to be a function of arity one/, fn ->
      Producer.init(
        topic: "my-topic",
        subscription: "my-subscription",
        active_state_callback: fn -> :ok end
      )
    end
  end
end
