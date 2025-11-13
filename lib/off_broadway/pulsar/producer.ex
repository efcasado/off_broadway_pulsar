defmodule OffBroadway.Pulsar.Producer do
  @moduledoc false

  use GenStage

  alias Broadway.Message

  require Logger

  @doc """
  Starts an `OffBroadway.Pulsar` producer process linked to the current
  process.
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    # TO-DO: This could be started globally, outside of the producer.
    pulsar_opts = Keyword.fetch!(opts, :pulsar)
    {:ok, _pid} = Pulsar.start(pulsar_opts)

    topic = Keyword.fetch!(opts, :topic)
    subscription = Keyword.fetch!(opts, :subscription)

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        OffBroadway.Pulsar.Consumer,
        subscription_type: :Shared,
        init_args: [self()],
        # Disable automatic flow - Broadway will control demand
        flow_initial: 0
      )

    # Get the actual consumer process PID (not the supervisor)
    # Pulsar.start_consumer returns a ConsumerGroup supervisor PID
    # We need the actual Consumer process PID to call send_flow
    [pulsar_consumer | _] = Pulsar.get_consumers(consumer_group)

    state = %{
      pulsar_consumer: pulsar_consumer,
      demand: 0,
      buffer: []
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: pending_demand, pulsar_consumer: consumer} = state) do
    :ok = Pulsar.Consumer.send_flow(consumer, incoming_demand)
    dispatch_messages(%{state | demand: incoming_demand + pending_demand})
  end

  @impl GenStage
  def handle_info({:pulsar_message, message_info}, state) do
    new_buffer = [message_info | state.buffer]
    dispatch_messages(%{state | buffer: new_buffer})
  end

  defp dispatch_messages(%{demand: 0} = state) do
    {:noreply, [], state}
  end

  defp dispatch_messages(%{buffer: [], demand: demand} = state) do
    {:noreply, [], %{state | demand: demand}}
  end

  defp dispatch_messages(%{buffer: buffer, demand: demand, pulsar_consumer: consumer} = state) do
    buffer_fifo = Enum.reverse(buffer)
    {to_dispatch, remaining} = Enum.split(buffer_fifo, demand)

    broadway_messages = Enum.map(to_dispatch, &wrap_message(&1, consumer))

    new_buffer = Enum.reverse(remaining)
    new_demand = demand - length(to_dispatch)

    {:noreply, broadway_messages, %{state | buffer: new_buffer, demand: new_demand}}
  end

  defp wrap_message(message_info, consumer) do
    %Message{
      data: extract_payload(message_info.payload),
      metadata: %{
        message_id: message_info.message_id,
        command: message_info.command,
        metadata: message_info.metadata,
        broker_metadata: message_info.broker_metadata
      },
      acknowledger: {OffBroadway.Pulsar.Acknowledger, %{consumer: consumer}, message_info.message_id}
    }
  end

  defp extract_payload({_single_metadata, payload}), do: payload
end
