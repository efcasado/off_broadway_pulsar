defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer

  @event [:off_broadway_pulsar, :consumer, :active_state_changed]

  test "emits telemetry and notifies the listener when active state changes" do
    handler_id = {__MODULE__, self()}

    :ok =
      :telemetry.attach(
        handler_id,
        @event,
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    consumer_pid = spawn(fn -> :ok end)
    state = %{subscription: "my-subscription", active_state_listener: self()}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :active, consumer_pid, "my-topic"},
             state
           ) == {:noreply, [], state}

    assert_receive {:telemetry, @event, %{system_time: system_time}, metadata}
    assert is_integer(system_time)

    assert metadata == %{
             active_state: :active,
             topic: "my-topic",
             subscription: "my-subscription",
             consumer_pid: consumer_pid,
             producer_pid: self()
           }

    assert_receive {:off_broadway_pulsar, :consumer_active_state_changed, ^metadata}
  end

  test "emits telemetry when no listener is configured" do
    handler_id = {__MODULE__, self()}

    :ok =
      :telemetry.attach(
        handler_id,
        @event,
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    state = %{subscription: "my-subscription", active_state_listener: nil}

    assert Producer.handle_info(
             {:consumer_active_state_changed, :passive, self(), "my-topic"},
             state
           ) == {:noreply, [], state}

    assert_receive {:telemetry, @event, _measurements, %{active_state: :passive}}
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end
end
