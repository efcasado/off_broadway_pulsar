defmodule OffBroadway.Pulsar.Producer do
  @moduledoc """
  A Broadway producer for Apache Pulsar.

  This producer receives messages from Pulsar topics and forwards them to the
  Broadway pipeline. It implements flow control using Pulsar's permit window
  mechanism, which proactively requests batches of messages rather than
  requesting per-message.

  Supports two connection patterns:
  - **Producer-managed**: Pass `:host` to have the producer start its own Pulsar connection
  - **Application-managed**: Omit `:host` to use a globally started Pulsar connection

  See `start_link/1` for detailed configuration options.
  """

  use GenStage

  alias Broadway.Message

  require Logger

  @supported_conn_opts [
    :socket_opts,
    :auth,
    :conn_timeout
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
    :startup_delay_ms,
    :startup_jitter_ms,
    :max_pending_chunked_messages,
    :expire_incomplete_chunked_message_after,
    :chunk_cleanup_interval,
    :read_compacted,
    :partition_discovery_interval_ms
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
  - `:client` - Client name to use when `:host` is not provided (optional, default: `:default`).
    Only used when connecting to a globally started Pulsar instance.
  - `:topic` - A single Pulsar topic to consume from (required if `:topics` is not set)
  - `:topics` - A list of Pulsar topics to consume from (required if `:topic` is not set).
    One consumer is started per topic. Providing a single topic via `:topic` is equivalent
    to `topics: [topic]` and is kept for backwards compatibility.
  - `:subscription` - The subscription name (required)
  - `:active_state_callback` - An optional `{module, function, extra_args}` tuple that receives
    active/passive state changes for Failover consumers. See "Failover Active State" below.
  - `:conn_opts` - Connection options passed to `Pulsar.start/1` (optional, only used if `:host` is provided):
    - `:socket_opts` - Socket options (e.g., `[verify: :verify_none]`)
    - `:auth` - Authentication configuration
    - `:conn_timeout` - Connection timeout in milliseconds
  - `:consumer_opts` - Consumer-specific options passed to `Pulsar.start_consumer/4` (optional).
    Applied to all topics when using `:topics`.
    - `:subscription_type` - Subscription type (`:Exclusive`, `:Failover`, `:Shared`, `:Key_Shared`, default: `:Shared`)
    - `:initial_position` - Initial position (`:latest` or `:earliest`, default: `:latest`)
    - `:durable` - Whether subscription is durable (default: `true`)
    - `:force_create_topic` - Force topic creation (default: `true`)
    - `:start_message_id` - Start from specific message ID
    - `:start_timestamp` - Start from timestamp
    - `:redelivery_interval` - Redelivery interval in milliseconds for NACKed messages
    - `:dead_letter_policy` - Dead letter queue configuration
    - `:startup_delay_ms` - Fixed startup delay in milliseconds before consumer initialization (default: 0)
    - `:startup_jitter_ms` - Random startup delay (0 to N ms) to avoid thundering herd on consumer restart (default: 0)
    - `:max_pending_chunked_messages` - Maximum number of concurrent chunked messages to buffer (default: 10)
    - `:expire_incomplete_chunked_message_after` - Timeout in milliseconds for incomplete chunked messages (default: 60_000)
    - `:chunk_cleanup_interval` - Interval in milliseconds for checking expired chunked messages (default: 30_000)
    - `:read_compacted` - If true, reads messages from the compacted topic ledger (default: false)
    - `:partition_discovery_interval_ms` - How often (in milliseconds) to poll for new partitions on
      partitioned topics. Set to `false` to disable discovery (default: 60_000)

  The total consumer startup delay is `startup_delay_ms + random(0, startup_jitter_ms)`, applied on every consumer start/restart.

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

  ## Failover Active State

  Consumers using a `:Failover` subscription report broker-provided active and
  passive state changes through the optional `:active_state_callback`. Configure
  the callback as a `{module, function, extra_args}` tuple. It is invoked as
  `apply(module, function, [metadata | extra_args])`, where `metadata` contains:

  - `:active_state` - Either `:active` or `:passive`
  - `:topic` - The topic, or individual partition topic, whose state changed
  - `:subscription` - The Pulsar subscription name
  - `:consumer_pid` - The underlying Pulsar consumer process

  The module must be loadable and the function must be exported with arity
  `length(extra_args) + 1`; this is validated when the producer initializes.

  The callback runs synchronously in the underlying Pulsar consumer, should return
  promptly, and propagates failures to that consumer. Reports are best-effort
  state observations: the same state may be reported more than once, and
  consumer termination does not guarantee a final `:passive` report. Treat
  reports idempotently and monitor `:consumer_pid` if local work must stop when
  the consumer exits.

  This signal is scoped to an individual topic or partition. It is not a
  distributed lock or fencing mechanism, and it does not mean messages already
  delivered to Broadway have finished processing.

  ## Usage Patterns

  ### Pattern 1: Single topic

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          host: "pulsar://localhost:6650",
          topic: "my-topic",
          subscription: "my-subscription"
        }
      ]

  ### Pattern 2: Multiple topics

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          host: "pulsar://localhost:6650",
          topics: ["topic-a", "topic-b", "topic-c"],
          subscription: "my-subscription"
        }
      ]

  ### Pattern 3: Application-managed connection (no host)

      # In your application.ex:
      children = [
        {Pulsar, host: "pulsar://localhost:6650"},
        MyApp.PulsarPipeline
      ]

      # In your producer config:
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topics: ["topic-a", "topic-b"],
          subscription: "my-subscription"
        }
      ]
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    # Accept either :topic (single, backwards-compatible) or :topics (list).
    # Both are normalized to a list internally.
    topics =
      cond do
        Keyword.has_key?(opts, :topics) ->
          Keyword.fetch!(opts, :topics)

        Keyword.has_key?(opts, :topic) ->
          [Keyword.fetch!(opts, :topic)]

        true ->
          raise ArgumentError, "either :topic or :topics is required"
      end

    subscription = Keyword.fetch!(opts, :subscription)
    client = Keyword.get(opts, :client, :default)

    active_state_callback =
      opts
      |> Keyword.get(:active_state_callback)
      |> validate_active_state_callback!()

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

    consumer_opts_base =
      opts
      |> Keyword.get(:consumer_opts, @default_consumer_opts)
      |> Keyword.take(@supported_consumer_opts)
      |> Keyword.put(:flow_initial, 0)
      |> Keyword.put(:client, client)

    # Start one consumer group per topic. Each group gets a unique name to
    # support both multi-topic and producer concurrency > 1.
    # consumer_registry is passed so each partition consumer can look up its
    # ConsumerGroup name and derive the partition topic as its stable map key.
    consumer_registry = Pulsar.Client.consumer_registry(client)

    consumer_groups =
      Enum.map(topics, fn topic ->
        unique_name = "#{topic}-#{subscription}-#{System.unique_integer([:positive])}"

        topic_opts =
          consumer_opts_base
          |> Keyword.put(:name, unique_name)
          |> Keyword.put(
            :init_args,
            [self(), topic, consumer_registry, subscription, active_state_callback]
          )

        {:ok, group} =
          Pulsar.start_consumer(
            topic,
            subscription,
            OffBroadway.Pulsar.Consumer,
            topic_opts
          )

        group
      end)

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
      consumer_groups: consumer_groups,
      # Populated via :consumer_ready as each consumer starts.
      # Keyed by topic (stable string) rather than consumer PID so that
      # consumer restarts simply overwrite the entry without leaving stale PIDs.
      # Maps topic => {consumer_pid, outstanding_permits}.
      consumers: %{},
      demand: 0,
      # Buffer entries are {%Pulsar.Message{}, consumer_pid, topic} triples:
      # consumer_pid routes ACKs/NACKs, topic routes flow-permit accounting.
      buffer: [],
      flow_initial: flow_initial,
      flow_threshold: flow_threshold,
      flow_refill: flow_refill
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    # With permit window, demand doesn't trigger flow requests
    # Permits are already requested proactively - just track demand
    new_demand = state.demand + incoming_demand
    Logger.debug("Received demand #{incoming_demand}, total demand: #{new_demand}, buffer: #{length(state.buffer)}")
    dispatch_messages(%{state | demand: new_demand})
  end

  @impl GenStage
  def handle_info({:consumer_ready, consumer_pid, topic}, state) do
    Logger.debug(
      "Consumer #{inspect(consumer_pid)} is ready for topic #{topic}, sending initial flow of #{state.flow_initial} permits"
    )

    case Pulsar.Consumer.send_flow(consumer_pid, state.flow_initial) do
      :ok ->
        Logger.debug("Sent initial flow of #{state.flow_initial} permits to consumer #{inspect(consumer_pid)}")

        # Keying by topic means a restarted consumer simply overwrites the old
        # entry. The stale PID is discarded automatically — no leak.
        consumers = Map.put(state.consumers, topic, {consumer_pid, state.flow_initial})
        {:noreply, [], %{state | consumers: consumers}}

      {:error, reason} ->
        Logger.error("Failed to send initial flow to consumer #{inspect(consumer_pid)}: #{inspect(reason)}")

        # Don't crash producer, consumer will retry or supervisor will handle it
        {:noreply, [], state}
    end
  end

  def handle_info({:pulsar_message, message_info, consumer_pid, topic}, state) do
    # consumer_pid routes ACK/NACK; topic routes flow-permit accounting.
    new_buffer = state.buffer ++ [{message_info, consumer_pid, topic}]
    Logger.debug("Message arrived, buffer size: #{length(new_buffer)}")

    dispatch_messages(%{state | buffer: new_buffer})
  end

  defp dispatch_messages(%{demand: 0} = state) do
    {:noreply, [], state}
  end

  defp dispatch_messages(%{buffer: [], demand: demand} = state) do
    {:noreply, [], %{state | demand: demand}}
  end

  defp dispatch_messages(%{buffer: buffer, demand: demand} = state) do
    {to_dispatch, to_drop, remaining} = pull_messages(buffer, demand)

    broadway_messages = Enum.map(to_dispatch, fn {msg, pid, _topic} -> wrap_message(msg, pid) end)
    Logger.debug("Dispatching #{length(to_dispatch)} messages, #{length(remaining)} remaining in buffer")

    new_demand = demand - length(to_dispatch)

    # Decrement outstanding permits per topic based on how many broker
    # messages each topic contributed to this dispatch batch.
    consumed = permits_consumed(to_dispatch ++ to_drop)

    consumers =
      Enum.reduce(consumed, state.consumers, fn {topic, count}, acc ->
        Map.update(acc, topic, {nil, 0}, fn {pid, outstanding} ->
          {pid, max(outstanding - count, 0)}
        end)
      end)

    state = %{state | buffer: remaining, demand: new_demand, consumers: consumers}
    state = maybe_refill_flow(state)

    {:noreply, broadway_messages, state}
  end

  defp pull_messages(buffer, demand) do
    {to_dispatch, to_drop, remaining, _count} =
      Enum.reduce(buffer, {[], [], [], 0}, fn {msg, _pid, _topic} = entry, {dispatch, drop, rest, count} ->
        cond do
          match?(%{chunked: true, complete: false}, msg) ->
            {dispatch, [entry | drop], rest, count}

          count < demand ->
            {[entry | dispatch], drop, rest, count + 1}

          true ->
            {dispatch, drop, [entry | rest], count}
        end
      end)

    {
      Enum.reverse(to_dispatch),
      Enum.reverse(to_drop),
      Enum.reverse(remaining)
    }
  end

  # Returns a map of topic => broker_message_count for the given buffer entries,
  # used to decrement each topic's consumer outstanding permits.
  defp permits_consumed(entries) do
    entries
    |> Enum.group_by(fn {_msg, _pid, topic} -> topic end)
    |> Map.new(fn {topic, items} ->
      count = Enum.sum(Enum.map(items, fn {msg, _, _} -> Pulsar.Message.num_broker_messages(msg) end))
      {topic, count}
    end)
  end

  defp wrap_message(%Pulsar.Message{} = pulsar_message, consumer) do
    %Message{
      data: pulsar_message.payload,
      metadata: %{
        message_id: pulsar_message.message_id_to_ack,
        command: pulsar_message.command,
        metadata: pulsar_message.metadata,
        single_metadata: pulsar_message.single_metadata,
        broker_metadata: pulsar_message.broker_metadata
      },
      acknowledger: {OffBroadway.Pulsar.Acknowledger, %{consumer: consumer}, pulsar_message.message_id_to_ack}
    }
  end

  # Checks each topic's consumer independently and refills its permit window
  # if it has dropped below the threshold.
  defp maybe_refill_flow(state) do
    consumers =
      Enum.reduce(state.consumers, state.consumers, fn {topic, {pid, outstanding}}, acc ->
        if outstanding <= state.flow_threshold do
          refill_consumer(acc, topic, pid, outstanding, state.flow_refill)
        else
          acc
        end
      end)

    %{state | consumers: consumers}
  end

  defp refill_consumer(consumers, topic, pid, outstanding, refill_amount) do
    case Pulsar.Consumer.send_flow(pid, refill_amount) do
      :ok ->
        new_outstanding = outstanding + refill_amount

        Logger.debug(
          "Refilled flow window for #{topic}: #{refill_amount} permits " <>
            "(outstanding: #{outstanding} → #{new_outstanding})"
        )

        Map.put(consumers, topic, {pid, new_outstanding})

      {:error, reason} ->
        Logger.error("Failed to refill flow window for #{topic}: #{inspect(reason)}")
        consumers
    end
  end

  # Validation helpers
  defp validate_positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  defp validate_positive_integer!(value, name) do
    raise ArgumentError,
          "expected :#{name} to be a positive integer, got: #{inspect(value)}"
  end

  defp validate_active_state_callback!(nil), do: nil

  defp validate_active_state_callback!({module, function, extra_args} = callback)
       when is_atom(module) and is_atom(function) and is_list(extra_args) do
    arity = length(extra_args) + 1

    if !(Code.ensure_loaded?(module) and function_exported?(module, function, arity)) do
      raise ArgumentError,
            "expected :active_state_callback #{inspect(module)}.#{function}/#{arity} to be exported"
    end

    callback
  end

  defp validate_active_state_callback!(callback) do
    raise ArgumentError,
          "expected :active_state_callback to be a {module, function, extra_args} tuple, " <>
            "got: #{inspect(callback)}"
  end
end
