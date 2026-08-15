defmodule OffBroadway.Pulsar.Producer do
  @moduledoc """
  A Broadway producer for Apache Pulsar.

  This producer receives messages from Pulsar topics and forwards them to the
  Broadway pipeline. It implements flow control using Pulsar's permit window
  mechanism, which proactively requests batches of messages rather than
  requesting per-message.

  The connection belongs to your application. Supervise a `Pulsar.Client` and name it
  with `:client`; this producer attaches its consumers to it.

  See `start_link/1` for detailed configuration options.
  """

  use GenStage

  alias Broadway.Message

  require Logger

  # Excludes :consumer_count, whose extra workers would all report the same resolved
  # topic and corrupt permit accounting, and the :flow_* options, which this producer
  # replaces by forcing flow_initial: 0.
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
    :batch_index_ack_enabled,
    :schema,
    :partition_discovery_interval_ms
  ]

  @default_consumer_opts [
    subscription_type: :shared
  ]

  # Flow control defaults - matches pulsar-elixir naming convention
  @default_flow_initial 100
  @default_flow_threshold 50
  @default_flow_refill 50

  @doc """
  Starts an `OffBroadway.Pulsar` producer process linked to the current
  process.

  ## Configuration

  - `:client` - Name of the `Pulsar.Client` to attach to (optional, default: `:default`).
    The client must already be running; see "Starting a client" below.
  - `:topic` - A single Pulsar topic to consume from (required if `:topics` is not set)
  - `:topics` - A list of Pulsar topics to consume from (required if `:topic` is not set).
    One consumer is started per topic. Providing a single topic via `:topic` is equivalent
    to `topics: [topic]` and is kept for backwards compatibility.
  - `:subscription` - The subscription name (required)
  - `:active_state_callback` - An optional `{module, function, extra_args}` tuple that receives
    active/passive state changes for Failover consumers. See "Failover Active State" below.
  - `:consumer_opts` - Consumer-specific options passed to `Pulsar.Consumer.start/1` (optional).
    Applied to all topics when using `:topics`.
    - `:subscription_type` - Subscription type (`:exclusive`, `:failover`, `:shared`, `:key_shared`, default: `:shared`)
    - `:initial_position` - Initial position (`:latest` or `:earliest`, default: `:latest`)
    - `:durable` - Whether subscription is durable (default: `true`)
    - `:force_create_topic` - Force topic creation (default: `true`)
    - `:start_message_id` - Start from specific `{ledger_id, entry_id}`
    - `:start_timestamp` - Start from timestamp
    - `:redelivery_interval` - Redelivery interval in milliseconds for NACKed messages
    - `:dead_letter_policy` - Dead letter queue configuration
    - `:startup_delay_ms` - Fixed startup delay in milliseconds before consumer initialization (default: 0)
    - `:startup_jitter_ms` - Random startup delay (0 to N ms) to avoid thundering herd on consumer restart (default: 0)
    - `:max_pending_chunked_messages` - Maximum number of concurrent chunked messages to buffer (default: 10)
    - `:expire_incomplete_chunked_message_after` - Timeout in milliseconds for incomplete chunked messages (default: 60_000)
    - `:chunk_cleanup_interval` - Interval in milliseconds for checking expired chunked messages (default: 30_000)
    - `:read_compacted` - If true, reads messages from the compacted topic ledger (default: false)
    - `:batch_index_ack_enabled` - Acknowledge individual messages within a batched entry rather
      than the whole entry (default: false). Worth enabling for Broadway, which routinely
      completes messages from one batch out of order, but it requires
      `acknowledgmentAtBatchIndexLevelEnabled=true` on the broker
    - `:schema` - Schema to register with the subscription, as `[type: atom, definition: term]`
    - `:partition_discovery_interval_ms` - How often (in milliseconds) to poll for new partitions on
      partitioned topics. Set to `false` to disable discovery (default: 60_000)

  `:consumer_count` is not accepted here; use Broadway's `producer: [concurrency: N]`.
  Nor are the consumer's own `:flow_*` options — see "Flow Control Options" below.

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

  ## Message Metadata

  Each `Broadway.Message` carries the following `:metadata`:

  - `:message_id` - Opaque id the acknowledger acks with. A *list* for a chunked message
  - `:message_id_string` - The id as Pulsar prints it (`ledgerId:entryId:partition`, plus the
    batch index when batched), for logging and correlation
  - `:topic` - The topic consumed from; the concrete partition for a partitioned topic, which
    makes it the right key for per-partition metrics
  - `:base_topic` - The topic the consumer was configured with. Equal to `:topic` unless the
    topic is partitioned; the one to route business logic on
  - `:partition` - The partition index, or `nil` when the topic is not partitioned
  - `:subscription` - The Pulsar subscription name
  - `:key` - The partition key, or `nil`
  - `:ordering_key` - The ordering key, or `nil`
  - `:properties` - User properties, as a map
  - `:producer_name` - The producer that published the message
  - `:publish_time` - Broker publish timestamp, in milliseconds since the epoch
  - `:event_time` - Application-set event time, or `nil` when unset
  - `:redelivery_count` - How many times the broker has redelivered the message
  - `:raw` - The underlying protocol structs, as a map of `:command`, `:metadata`,
    `:single_metadata` and `:broker_metadata`. **Unstable**: its shape follows the wire
    protocol. Reach for it only for details the fields above do not cover

  Those fields come from `Pulsar.Message`, so they answer the same way whether a message
  arrived on its own, inside a batch, or split across chunks.

  ## Failover Active State

  Consumers using a `:failover` subscription report broker-provided active and
  passive state changes through the optional `:active_state_callback`. Configure
  the callback as a `{module, function, extra_args}` tuple. It is invoked as
  `apply(module, function, [metadata | extra_args])`, where `metadata` contains:

  - `:active_state` - Either `:active` or `:passive`
  - `:topic` - The topic, or individual partition topic, whose state changed
  - `:subscription` - The Pulsar subscription name
  - `:consumer_pid` - The underlying Pulsar consumer process

  The callback runs synchronously in the underlying Pulsar consumer and should
  return promptly. Exceptions raised by the callback propagate and crash the
  consumer. Reports are best-effort state observations: the same state may be
  reported more than once, and consumer termination does not guarantee a final
  `:passive` report. Treat reports idempotently and monitor `:consumer_pid` if
  local work must stop when the consumer exits.

  This signal is scoped to an individual topic or partition. It is not a
  distributed lock or fencing mechanism, and it does not mean messages already
  delivered to Broadway have finished processing.

  With Broadway producer concurrency greater than one, multiple underlying
  consumers can report different states for the same topic and subscription at
  the same time. Track concurrent ownership observations by `:consumer_pid`, not
  by topic alone.

  ## Starting a client

  Supervise the client above the pipeline, so the connection outlives any one producer
  stage and is shared by all of them:

      children = [
        {Pulsar.Client, host: "pulsar://localhost:6650"},
        MyApp.PulsarPipeline
      ]

  A single topic:

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topic: "my-topic",
          subscription: "my-subscription"
        }
      ]

  Or several, one consumer each:

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topics: ["topic-a", "topic-b", "topic-c"],
          subscription: "my-subscription"
        }
      ]

  Name the client to run more than one, or to consume from more than one cluster:

      children = [
        {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
        MyApp.AnalyticsPipeline
      ]

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          client: :analytics,
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

    active_state_callback = Keyword.get(opts, :active_state_callback)

    ensure_client_running!(client)

    consumer_opts_base =
      opts
      |> Keyword.get(:consumer_opts, @default_consumer_opts)
      |> Keyword.take(@supported_consumer_opts)
      |> Keyword.put(:flow_initial, 0)
      |> Keyword.put(:client, client)

    # Start one consumer per topic. Each gets a unique name to support both
    # multi-topic and producer concurrency > 1. They run under the client's
    # supervisor, not under this producer.
    Enum.each(topics, fn topic ->
      unique_name = "#{topic}-#{subscription}-#{System.unique_integer([:positive])}"

      topic_opts =
        Keyword.merge(consumer_opts_base,
          topic: topic,
          subscription_name: subscription,
          callback_module: OffBroadway.Pulsar.Consumer,
          name: unique_name,
          init_args: [self(), active_state_callback]
        )

      {:ok, _consumer} = Pulsar.Consumer.start(topic_opts)
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
      # topic => {consumer_pid, outstanding_permits}, populated via :consumer_ready.
      # Keyed by resolved topic rather than PID so a restarted consumer overwrites
      # its entry instead of leaving a stale one behind.
      consumers: %{},
      demand: 0,
      # {%Pulsar.Message{}, consumer_pid, context} triples: consumer_pid routes
      # ACKs/NACKs, context.topic routes flow-permit accounting.
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
  def handle_info({:consumer_ready, consumer_pid, %{topic: topic}}, state) do
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

  def handle_info({:pulsar_message, message_info, consumer_pid, context}, state) do
    # consumer_pid routes ACK/NACK; context.topic routes flow-permit accounting.
    new_buffer = state.buffer ++ [{message_info, consumer_pid, context}]
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

    broadway_messages = Enum.map(to_dispatch, fn {msg, pid, context} -> wrap_message(msg, pid, context) end)
    Logger.debug("Dispatching #{length(to_dispatch)} messages, #{length(remaining)} remaining in buffer")

    new_demand = demand - length(to_dispatch)

    discard(to_drop)

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
      Enum.reduce(buffer, {[], [], [], 0}, fn {msg, _pid, _context} = entry, {dispatch, drop, rest, count} ->
        cond do
          # Nothing the pipeline can do with it, so it goes to `drop` rather than `rest`:
          # dropped entries are acked and still counted against the permit window.
          not deliverable?(msg) ->
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

  # An incomplete chunked message has only part of its payload and an invalid one has
  # payload bytes that failed validation. Neither can be handed to the pipeline.
  defp deliverable?(msg), do: Pulsar.Message.complete?(msg) and Pulsar.Message.valid?(msg)

  # Undeliverable messages never reach the acknowledger, so they are acked here.
  # Leaving them unacked would hold the subscription's cursor behind them for the
  # life of the consumer; there is no point nacking, since redelivery cannot fix
  # a partial payload or a failed checksum.
  defp discard(entries) do
    Enum.each(entries, fn {msg, consumer_pid, _context} ->
      case Pulsar.Consumer.ack(consumer_pid, msg.message_id) do
        :ok -> :ok
        {:error, reason} -> Logger.error("Failed to ack undeliverable message: #{inspect(reason)}")
      end
    end)
  end

  # Returns a map of topic => broker_message_count for the given buffer entries,
  # used to decrement each topic's consumer outstanding permits.
  defp permits_consumed(entries) do
    entries
    |> Enum.group_by(fn {_msg, _pid, context} -> context.topic end)
    |> Map.new(fn {topic, items} ->
      count = Enum.sum(Enum.map(items, fn {msg, _, _} -> Pulsar.Message.num_broker_messages(msg) end))
      {topic, count}
    end)
  end

  defp wrap_message(%Pulsar.Message{} = pulsar_message, consumer, context) do
    %Message{
      data: pulsar_message.payload,
      metadata: %{
        message_id: pulsar_message.message_id,
        message_id_string: Pulsar.Message.message_id_string(pulsar_message),
        topic: context.topic,
        base_topic: context.base_topic,
        partition: context.partition,
        subscription: context.subscription_name,
        key: Pulsar.Message.key(pulsar_message),
        ordering_key: Pulsar.Message.ordering_key(pulsar_message),
        properties: Pulsar.Message.properties(pulsar_message),
        producer_name: Pulsar.Message.producer_name(pulsar_message),
        publish_time: Pulsar.Message.publish_time(pulsar_message),
        event_time: Pulsar.Message.event_time(pulsar_message),
        redelivery_count: Pulsar.Message.redelivery_count(pulsar_message),
        raw: pulsar_message.raw
      },
      acknowledger: {OffBroadway.Pulsar.Acknowledger, %{consumer: consumer}, pulsar_message.message_id}
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

  # Without this, an unstarted client surfaces as a :noproc exit from deep inside the
  # client's supervisor when the first consumer is started.
  defp ensure_client_running!(client) do
    if is_nil(Process.whereis(client)) do
      raise ArgumentError, """
      Pulsar client #{inspect(client)} is not running.

      Supervise it above the pipeline:

          children = [
            {Pulsar.Client, name: #{inspect(client)}, host: "pulsar://localhost:6650"},
            MyApp.PulsarPipeline
          ]
      """
    end
  end

  # Validation helpers
  defp validate_positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  defp validate_positive_integer!(value, name) do
    raise ArgumentError,
          "expected :#{name} to be a positive integer, got: #{inspect(value)}"
  end
end
