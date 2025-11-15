defmodule OffBroadway.Pulsar.Producer do
  @moduledoc false

  use GenStage

  alias Broadway.Message

  require Logger

  @supported_conn_opts [
    :socket_opts,
    :auth,
    :conn_timeout,
    :startup_jitter_ms,
    :start_delay_ms
  ]

  @default_conn_opts []

  @supported_consumer_opts [
    :subscription_type,
    :initial_position,
    :durable,
    :force_create_topic,
    :start_message_id,
    :start_timestamp,
    :redelivery_interval,
    :dead_letter_policy,
    :consumer_count
  ]

  @default_consumer_opts [
    subscription_type: :Shared
  ]

  @doc """
  Starts an `OffBroadway.Pulsar` producer process linked to the current
  process.

  ## Configuration

  - `:host` - Broker URL (e.g., "pulsar://localhost:6650") (optional).
    If provided, the producer will start its own Pulsar connection.
    If not provided, the producer assumes Pulsar is already started globally
    (e.g., in your application's supervision tree).
  - `:topic` - The Pulsar topic to consume from (required)
  - `:subscription` - The subscription name (required)
  - `:conn_opts` - Connection options passed to `Pulsar.start/1` (optional, only used if `:host` is provided):
    - `:socket_opts` - Socket options (e.g., `[verify: :verify_none]`)
    - `:auth` - Authentication configuration
    - `:conn_timeout` - Connection timeout in milliseconds
    - `:startup_jitter_ms` - Random startup delay to avoid thundering herd
    - `:start_delay_ms` - Startup delay in milliseconds
  - `:consumer_opts` - Consumer-specific options passed to `Pulsar.start_consumer/4` (optional):
    - `:subscription_type` - Subscription type (`:Exclusive`, `:Shared`, `:Key_Shared`, default: `:Shared`)
    - `:initial_position` - Initial position (`:latest` or `:earliest`, default: `:latest`)
    - `:durable` - Whether subscription is durable (default: `true`)
    - `:force_create_topic` - Force topic creation (default: `true`)
    - `:start_message_id` - Start from specific message ID
    - `:start_timestamp` - Start from timestamp
    - `:redelivery_interval` - Redelivery interval in milliseconds for NACKed messages
    - `:dead_letter_policy` - Dead letter queue configuration
    - `:consumer_count` - Number of consumer processes (default: 1)

  Flow control options (`:flow_initial`, `:flow_threshold`, `:flow_refill`) are not supported
  in `:consumer_opts` because Broadway controls message flow.

  ## Usage Patterns

  ### Pattern 1: Producer-managed connection (host provided)
  The producer starts its own Pulsar connection. Useful for simple setups or when
  each producer needs different connection settings.

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          host: "pulsar://localhost:6650",
          topic: "my-topic",
          subscription: "my-subscription"
        }
      ]

  ### Pattern 2: Application-managed connection (no host)
  Pulsar is started globally in your supervision tree. Useful when multiple producers
  share the same cluster or for better resource management.

      # In your application.ex:
      children = [
        {Pulsar, host: "pulsar://localhost:6650"},
        MyApp.PulsarPipeline
      ]

      # In your producer config:
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topic: "my-topic",
          subscription: "my-subscription"
        }
      ]
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    subscription = Keyword.fetch!(opts, :subscription)

    # Only start Pulsar if host is provided (producer-managed connection)
    # Otherwise, assume Pulsar is already started globally (application-managed connection)
    case Keyword.fetch(opts, :host) do
      {:ok, host} ->
        conn_opts =
          opts
          |> Keyword.get(:conn_opts, @default_conn_opts)
          |> Keyword.take(@supported_conn_opts)

        pulsar_opts = Keyword.put(conn_opts, :host, host)

        {:ok, _pid} = Pulsar.start(pulsar_opts)

      :error ->
        :ok
    end

    consumer_opts =
      opts
      |> Keyword.get(:consumer_opts, @default_consumer_opts)
      |> Keyword.take(@supported_consumer_opts)
      |> Keyword.put(:init_args, [self()])
      |> Keyword.put(:flow_initial, 0)

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        OffBroadway.Pulsar.Consumer,
        consumer_opts
      )

    # TO-DO: move this logic to pulsar-elixir. Consumer Group should
    # know how to route messages to the right consumer. Alternatively,
    # use named consumers so that we can ACK/NACK messages by name.
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
