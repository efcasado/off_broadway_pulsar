defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  @impl true
  def init([broadway_producer, topic]) do
    # Notify producer that consumer is ready and needs initial flow.
    # topic is included so the producer can use it as a stable map key,
    # avoiding stale PID accumulation when this consumer restarts.
    # This is sent on first startup AND on every consumer restart.
    send(broadway_producer, {:consumer_ready, self(), topic})
    {:ok, %{broadway_producer: broadway_producer, topic: topic}}
  end

  @impl true
  def handle_message(%Pulsar.Message{} = message, state) do
    # self() is the consumer PID used for ACK/NACK routing.
    # state.topic is the stable key used for flow-permit accounting.
    send(state.broadway_producer, {:pulsar_message, message, self(), state.topic})

    # Return {:noreply, state} to use manual ACK mode
    # Broadway will handle ACK/NACK through the acknowledger
    {:noreply, state}
  end
end
