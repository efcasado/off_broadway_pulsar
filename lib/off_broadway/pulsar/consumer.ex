defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  @behaviour Pulsar.Consumer.Callback

  @impl true
  def init([broadway_producer]) do
    # Notify producer that consumer is ready and needs initial flow
    # This is sent on first startup AND on every consumer restart
    send(broadway_producer, {:consumer_ready, self()})
    {:ok, %{broadway_producer: broadway_producer}}
  end

  @impl true
  def handle_message(%Pulsar.Message{} = message, state) do
    # Send the Pulsar.Message struct directly to the Broadway producer
    send(state.broadway_producer, {:pulsar_message, message})

    # Return {:noreply, state} to use manual ACK mode
    # Broadway will handle ACK/NACK through the acknowledger
    {:noreply, state}
  end
end
