defmodule OffBroadway.Pulsar.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @impl Broadway.Acknowledger
  def ack(%{consumer: consumer}, successful, failed) do
    Enum.each(successful, fn %{acknowledger: {_, _, message_id}} ->
      :ok = Pulsar.ack(consumer, message_id)
    end)

    Enum.each(failed, fn %{acknowledger: {_, _, message_id}} ->
      :ok = Pulsar.nack(consumer, message_id)
    end)
  end
end
