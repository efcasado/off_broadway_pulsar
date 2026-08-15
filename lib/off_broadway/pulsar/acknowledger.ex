defmodule OffBroadway.Pulsar.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @impl Broadway.Acknowledger
  def ack(%{consumer: consumer}, successful, failed) do
    ack_ids =
      Enum.flat_map(successful, fn %{acknowledger: {_, _, message_id}} ->
        List.wrap(message_id)
      end)

    :ok = Pulsar.Consumer.ack(consumer, ack_ids)

    nack_ids =
      Enum.flat_map(failed, fn %{acknowledger: {_, _, message_id}} ->
        List.wrap(message_id)
      end)

    :ok = Pulsar.Consumer.nack(consumer, nack_ids)
  end
end
