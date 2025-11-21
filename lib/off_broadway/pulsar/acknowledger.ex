defmodule OffBroadway.Pulsar.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @impl Broadway.Acknowledger
  def ack(%{consumer: consumer}, successful, failed) do
    ack_ids = Enum.map(successful, fn %{acknowledger: {_, _, message_id}} -> message_id end)
    :ok = Pulsar.ack(consumer, ack_ids)

    nack_ids = Enum.map(failed, fn %{acknowledger: {_, _, message_id}} -> message_id end)
    :ok = Pulsar.nack(consumer, nack_ids)
  end
end
