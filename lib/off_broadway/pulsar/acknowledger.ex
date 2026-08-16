defmodule OffBroadway.Pulsar.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  require Logger

  @impl Broadway.Acknowledger
  def ack(%{consumer: consumer}, successful, failed) do
    acknowledge(consumer, :ack, message_ids(successful))
    acknowledge(consumer, :nack, message_ids(failed))

    :ok
  end

  defp message_ids(messages) do
    Enum.flat_map(messages, fn %{acknowledger: {_, _, message_id}} ->
      List.wrap(message_id)
    end)
  end

  defp acknowledge(_consumer, _operation, []), do: :ok

  # Broadway acknowledges every worker of a batch from one process and lets an exit abort the
  # ones it has not reached, so a worker that has already been replaced would take its live
  # siblings' acknowledgements with it. Its own messages need none: the broker returns what the
  # old worker left unacknowledged to the replacement.
  defp acknowledge(consumer, operation, message_ids) do
    case apply(Pulsar.Consumer, operation, [consumer, message_ids]) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Pulsar #{operation} of #{length(message_ids)} message(s) failed: #{inspect(reason)}")
    end
  catch
    :exit, reason ->
      Logger.warning(
        "Dropping #{operation} of #{length(message_ids)} message(s): " <>
          "consumer #{inspect(consumer)} is gone (#{inspect(reason)})"
      )
  end
end
