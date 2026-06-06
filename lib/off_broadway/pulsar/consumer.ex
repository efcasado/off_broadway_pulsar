defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  @impl true
  def init([broadway_producer, base_topic, consumer_registry]) do
    # Notify producer that consumer is ready and needs initial flow.
    # topic is included so the producer can use it as a stable map key,
    # avoiding stale PID accumulation when this consumer restarts.
    # This is sent on first startup AND on every consumer restart.
    #
    # For partitioned topics, resolve_topic/3 derives the partition topic
    # so each partition gets a unique key; non-partitioned consumers get
    # base_topic unchanged.
    topic = resolve_topic(self(), base_topic, consumer_registry)
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

  # Finds the ConsumerGroup PID among this consumer's process links, looks up
  # its registered name in the consumer registry, and returns the partition topic
  # if the name carries a "-partition-N" suffix; otherwise returns base_topic.
  defp resolve_topic(consumer_pid, base_topic, consumer_registry) do
    {:links, links} = Process.info(consumer_pid, :links)

    Enum.find_value(links, base_topic, fn linked_pid ->
      case Registry.keys(consumer_registry, linked_pid) do
        [name] when is_binary(name) -> partition_topic(base_topic, name)
        _ -> nil
      end
    end)
  end

  defp partition_topic(base_topic, group_name) do
    case Regex.run(~r/-partition-(\d+)$/, group_name) do
      [_, idx_str] -> Pulsar.PartitionTopic.name(base_topic, String.to_integer(idx_str))
      nil -> nil
    end
  end
end
