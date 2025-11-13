defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  @behaviour Pulsar.Consumer.Callback

  @impl true
  def init([broadway_producer]) do
    {:ok, %{broadway_producer: broadway_producer}}
  end

  @impl true
  def handle_message({command, metadata, {msg_metadata, msg_payload}, broker_metadata, message_id_to_ack}, state) do
    # message_id_to_ack has the correct batch_index set for batch messages
    message_info = %{
      message_id: message_id_to_ack,
      command: command,
      metadata: metadata,
      payload: {msg_metadata, msg_payload},
      broker_metadata: broker_metadata
    }

    send(state.broadway_producer, {:pulsar_message, message_info})

    # Return {:noreply, state} to use manual ACK mode
    # Broadway will handle ACK/NACK through the acknowledger
    {:noreply, state}
  end
end
