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
    :dead_letter_policy
  ]

  @default_consumer_opts [
    subscription_type: :Shared
  ]

  # Flow control defaults - matches pulsar-elixir naming convention
  @default_flow_initial 100
  @default_flow_threshold 50
  @default_flow_refill 50

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

  ## Flow Control Options

  The producer uses Pulsar's native permit window flow control. These options
  match the naming convention used in `pulsar-elixir` consumer:

  - `:flow_initial` - Initial permits requested at startup (optional, default: 100)
  - `:flow_threshold` - Trigger refill when permits drop to this level (optional, default: 50)
  - `:flow_refill` - Number of permits to request on each refill (optional, default: 50)

  Note: These flow control options are for the Broadway producer level and override
  the consumer's automatic flow control (which is disabled by setting `flow_initial: 0`
  in the underlying consumer).

  Broadway processor demands are satisfied from the already-requested permit window,
  eliminating per-demand flow requests.

  When using `producer: [concurrency: N]` with N > 1, each producer maintains
  its own independent permit window.

  ## Usage Patterns

  ### Pattern 1: Producer-managed connection (host provided)

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          host: "pulsar://localhost:6650",
          topic: "my-topic",
          subscription: "my-subscription"
        }
      ]

  ### Pattern 2: Application-managed connection (no host)

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

    # Generate unique name for this producer instance to support producer concurrency
    # Without this, multiple producers would try to register with the same name
    unique_name = "#{topic}-#{subscription}-#{System.unique_integer([:positive])}"
    consumer_opts = Keyword.put(consumer_opts, :name, unique_name)

    {:ok, consumer_group} =
      Pulsar.start_consumer(
        topic,
        subscription,
        OffBroadway.Pulsar.Consumer,
        consumer_opts
      )

    # Get the actual consumer process PID (not the supervisor)
    [pulsar_consumer | _] = Pulsar.get_consumers(consumer_group)

    # Extract flow control configuration (matches pulsar-elixir naming)
    flow_initial =
      opts
      |> Keyword.get(:flow_initial, @default_flow_initial)
      |> validate_positive_integer!(:flow_initial)

    flow_threshold =
      opts
      |> Keyword.get(:flow_threshold, @default_flow_threshold)
      |> validate_positive_integer!(:flow_threshold)

    flow_refill =
      opts
      |> Keyword.get(:flow_refill, @default_flow_refill)
      |> validate_positive_integer!(:flow_refill)

    # Validate threshold < initial (otherwise refill never triggers)
    if flow_threshold >= flow_initial do
      raise ArgumentError,
            "flow_threshold (#{flow_threshold}) must be less than flow_initial (#{flow_initial})"
    end

    state = %{
      pulsar_consumer: pulsar_consumer,
      demand: 0,
      buffer: [],
      # Flow control state (Pulsar's native permit window)
      flow_initial: flow_initial,
      flow_threshold: flow_threshold,
      flow_refill: flow_refill,
      outstanding_permits: 0
    }

    # Send initial flow control permits
    state = send_initial_flow(state)

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    # With permit window, demand doesn't trigger flow requests
    # Permits are already requested proactively - just track demand
    new_demand = state.demand + incoming_demand
    dispatch_messages(%{state | demand: new_demand})
  end

  @impl GenStage
  def handle_info({:pulsar_message, message_info}, state) do
    # Add message to buffer but DON'T dispatch
    # Only dispatch when Broadway explicitly demands via handle_demand
    # This is critical for GenStage backpressure to work correctly
    new_buffer = [message_info | state.buffer]
    {:noreply, [], %{state | buffer: new_buffer}}
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
    
    # Decrement outstanding permits for dispatched messages
    dispatched_count = length(to_dispatch)
    new_outstanding = max(state.outstanding_permits - dispatched_count, 0)
    
    # Check if we need to refill after dispatching
    state = %{state | buffer: new_buffer, demand: new_demand, outstanding_permits: new_outstanding}
    state = maybe_refill_flow(state)

    {:noreply, broadway_messages, state}
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

  # Sends initial flow control permits at startup
  defp send_initial_flow(state) do
    case Pulsar.Consumer.send_flow(state.pulsar_consumer, state.flow_initial) do
      :ok ->
        Logger.info("Sent initial flow of #{state.flow_initial} permits to Pulsar consumer")

        %{state | outstanding_permits: state.flow_initial}

      {:error, reason} ->
        Logger.error("Failed to send initial flow: #{inspect(reason)}")
        state
    end
  end

  # Refills permit window when it drops below threshold
  defp maybe_refill_flow(state) do
    outstanding = state.outstanding_permits
    threshold = state.flow_threshold
    refill_amount = state.flow_refill

    if outstanding <= threshold do
      case Pulsar.Consumer.send_flow(state.pulsar_consumer, refill_amount) do
        :ok ->
          new_outstanding = outstanding + refill_amount

          Logger.debug(
            "Refilled flow window: #{refill_amount} permits (outstanding: #{outstanding} → #{new_outstanding})"
          )

          %{state | outstanding_permits: new_outstanding}

        {:error, reason} ->
          Logger.error("Failed to refill flow window: #{inspect(reason)}")
          state
      end
    else
      state
    end
  end

  # Validation helpers
  defp validate_positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  defp validate_positive_integer!(value, name) do
    raise ArgumentError,
          "expected :#{name} to be a positive integer, got: #{inspect(value)}"
  end
end
