defmodule OffBroadway.Pulsar.Producer do
  @moduledoc """
  A Broadway producer for Apache Pulsar.

  This producer receives messages from Pulsar topics and forwards them to the
  Broadway pipeline. It implements flow control using Pulsar's permit window
  mechanism, which proactively requests batches of messages rather than
  requesting per-message.

  The connection belongs to your application: supervise a `Pulsar.Client` and this
  producer attaches its consumers to it.

  The consumers are owned by the producer stage rather than by the client: they are
  started with `Pulsar.Consumer.start_link/1`, so the stage and the consumers feeding
  it share a fate. See "Consumer ownership" in `start_link/1`.

  See `start_link/1` for detailed configuration options.
  """

  use GenStage

  alias Broadway.Message
  alias OffBroadway.Pulsar.Consumer, as: Callback

  require Logger

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

  @default_flow_initial 100
  @default_flow_threshold 50
  @default_flow_refill 50

  # How often the stage checks on the consumers it owns. Neither failure it looks for sends
  # this stage anything: a worker that fails terminally before its callback runs was never
  # reported here, and a lost registration is silent by construction. See check_consumers/1.
  @health_check_interval_ms 5_000

  # A stage restarting because its client's consumer branch was replaced can arrive before
  # the replacement registry does. Waiting briefly rebuilds against it instead of raising
  # into Broadway's restart budget.
  @registry_wait_ms 2_000
  @registry_poll_ms 100

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
  - `:consumer_opts` - Consumer-specific options passed to `Pulsar.Consumer.start_link/1` (optional).
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

  - `:flow_initial` - Permits each consumer grants when it subscribes (optional, default: 100)
  - `:flow_threshold` - Refill once outstanding permits reach this level (optional, default: 50)
  - `:flow_refill` - Permits granted by each refill (optional, default: 50)

  These replace the consumer's own automatic refills: the producer sets the consumer's
  `:flow_policy` to report what each delivery cost and grants the refills itself, so the
  broker runs no further ahead than Broadway has asked for. Processor demand is served from
  the window already granted, rather than triggering a flow request of its own.

  Each consumer keeps its own window — one per topic, and one per partition of a
  partitioned topic. With `producer: [concurrency: N]`, each producer has its own
  consumers, and so its own windows.

  ## Consumer ownership

  Consumers are started with `Pulsar.Consumer.start_link/1`, which links each consumer root
  to the producer stage that started it. The link carries ownership in both directions:

  - The stage exits and its consumer roots exit with it, unsubscribing rather than
    lingering as orphans that hold the subscription and deliver to a dead pid.
  - A consumer root exits and the stage exits with it. Broadway restarts the stage, and
    `init/1` recreates the consumers.

  Three consequences are worth knowing:

  - The consumers are supervised by Broadway's producer supervisor, not by the client's
    consumer `DynamicSupervisor`, so `Pulsar.Client.consumers/1` does not list them. They
    are still registered in the client's consumer registry, so `Pulsar.Consumer.stop/2`
    and anything else resolving a consumer by name still finds them. Stopping one that way
    is undone: the stage notices it has no consumer and is restarted with a new one.
  - Failure that a consumer cannot recover from takes the pipeline down instead of leaving
    it running and silent. A consumer worker that stops on a terminal subscribe error
    (`:ConsumerBusy`, `:AuthorizationError`, `:TopicNotFound` and friends) stops the stage,
    as does the client's consumer branch being replaced underneath it. Ordinary worker
    crashes are left to the client's own supervision and do not disturb the stage.
  - Because the client's consumer registry holds the consumer names, the stage stops when it
    finds its own registration gone, so that `init/1` can re-register against the registry
    that replaced it.

  ## Message Metadata

  Each `Broadway.Message` carries the following `:metadata`:

  - `:message_id` - Opaque id the acknowledger acks with. A *list* for a chunked message
  - `:message_id_string` - The id as Pulsar prints it (`ledgerId:entryId:partition`, plus the
    batch index when batched), for logging and correlation
  - `:topic` - The resolved topic; the concrete partition for a partitioned topic
  - `:base_topic` - The configured topic. Equal to `:topic` unless the topic is partitioned
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
    `:single_metadata` and `:broker_metadata`. Its shape is unstable and follows the wire
    protocol

  These normalized fields are consistent for individual, batched and chunked messages.

  Two kinds of message never reach the pipeline: an incomplete chunked message, whose
  payload is a fragment, and one that failed validation. Both are logged, acknowledged
  and dropped.

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

      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topics: ["topic-a", "topic-b"],
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

    # Validated before any consumer is started, so a misconfigured stage never subscribes
    # only to raise on the option after it.
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

    if flow_threshold >= flow_initial do
      raise ArgumentError,
            "flow_threshold (#{flow_threshold}) must be less than flow_initial (#{flow_initial})"
    end

    # The consumer names are registered here, so a stage that got ahead of its client's
    # restart waits for the registry rather than raising into Broadway's restart budget.
    await_consumer_registry!(client)

    # Each worker grants its own initial window on subscribe, so restarts and
    # late-discovered partitions come back with permits rather than waiting to be given
    # any. Refills are this producer's, driven by what Broadway takes.
    consumer_opts_base =
      opts
      |> Keyword.get(:consumer_opts, @default_consumer_opts)
      |> Keyword.take(@supported_consumer_opts)
      |> Keyword.merge(
        client: client,
        flow_initial: flow_initial,
        flow_policy: {Callback, :report_permits, [self()]}
      )

    # The unique name keeps multi-topic and producer concurrency > 1 apart. Each root is
    # linked to this producer, which owns it; see "Consumer ownership".
    consumer_roots =
      Map.new(topics, fn topic ->
        unique_name = "#{topic}-#{subscription}-#{System.unique_integer([:positive])}"

        topic_opts =
          Keyword.merge(consumer_opts_base,
            topic: topic,
            subscription_name: subscription,
            callback_module: Callback,
            name: unique_name,
            init_args: [self(), active_state_callback]
          )

        {start_consumer!(topic_opts), {topic, unique_name}}
      end)

    state = %{
      # consumer_pid => {topic, outstanding_permits}, populated via :consumer_ready.
      # Permits belong to a worker instance, so the worker is the key; the topic only
      # rides along for logging.
      consumers: %{},
      # root pid => {topic, registered name}, for the consumers this stage owns. Linked, so
      # a root that exits takes the stage with it before any monitor would report it.
      consumer_roots: consumer_roots,
      client: client,
      demand: 0,
      # An ordered mix of {:message, %Pulsar.Message{}, consumer_pid, context} and
      # {:permits, consumer_pid, consumed} markers. See take_dispatchable/3.
      buffer: [],
      flow_initial: flow_initial,
      flow_threshold: flow_threshold,
      flow_refill: flow_refill
    }

    schedule_health_check()

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    new_demand = state.demand + incoming_demand
    Logger.debug("Received demand #{incoming_demand}, total demand: #{new_demand}, buffer: #{length(state.buffer)}")
    dispatch_messages(%{state | demand: new_demand})
  end

  @impl GenStage
  def handle_info({:consumer_ready, consumer_pid, %{topic: topic}}, state) do
    Logger.debug("Consumer #{inspect(consumer_pid)} is ready for topic #{topic} with #{state.flow_initial} permits")

    Process.monitor(consumer_pid)
    consumers = Map.put(state.consumers, consumer_pid, {topic, state.flow_initial})

    {:noreply, [], %{state | consumers: consumers}}
  end

  def handle_info({:pulsar_message, message_info, consumer_pid, context}, state) do
    new_buffer = state.buffer ++ [{:message, message_info, consumer_pid, context}]
    Logger.debug("Message arrived, buffer size: #{length(new_buffer)}")

    dispatch_messages(%{state | buffer: new_buffer})
  end

  def handle_info({:permits_consumed, consumer_pid, consumed}, state) do
    dispatch_messages(%{state | buffer: state.buffer ++ [{:permits, consumer_pid, consumed}]})
  end

  # A worker stopping with `{:shutdown, {code, message}}` hit an error the client refuses to
  # retry: a subscription already taken, credentials that stay rejected, a topic that is not
  # there. Its group is gone with it, so this stage has no consumer left for that topic.
  def handle_info({:DOWN, _ref, :process, consumer_pid, {:shutdown, {code, _message} = reason}}, state)
      when is_atom(code) do
    case state.consumers do
      %{^consumer_pid => {topic, _outstanding}} ->
        Logger.error("Consumer for #{topic} stopped on a terminal error: #{inspect(reason)}")

        {:stop, {:shutdown, {:consumer_terminal_error, topic, reason}}, state}

      _no_such_consumer ->
        forget_consumer(consumer_pid, state)
    end
  end

  def handle_info({:DOWN, _ref, :process, consumer_pid, _reason}, state) do
    forget_consumer(consumer_pid, state)
  end

  def handle_info(:check_consumers, state) do
    schedule_health_check()

    case check_consumers(state) do
      :ok ->
        {:noreply, [], state}

      {:gone, topic} ->
        Logger.error("Consumer for #{topic} is gone; stopping")

        {:stop, {:shutdown, {:consumer_gone, topic}}, state}

      {:stopped, topic} ->
        Logger.error("Consumer for #{topic} has a stopped group and no worker left; stopping")

        {:stop, {:shutdown, {:consumer_stopped, topic}}, state}

      {:unregistered, topic} ->
        Logger.error("Consumer for #{topic} is no longer registered with client #{inspect(state.client)}; stopping")

        {:stop, {:shutdown, {:consumer_unregistered, topic}}, state}
    end
  end

  defp forget_consumer(consumer_pid, state) do
    # Entries from a dead worker can no longer be acknowledged; discard them and
    # their permit markers.
    buffer =
      Enum.reject(state.buffer, fn
        {:message, _msg, ^consumer_pid, _context} -> true
        {:permits, ^consumer_pid, _consumed} -> true
        _entry -> false
      end)

    {:noreply, [],
     %{
       state
       | consumers: Map.delete(state.consumers, consumer_pid),
         buffer: buffer
     }}
  end

  defp dispatch_messages(%{buffer: buffer, demand: demand} = state) do
    {taken, remaining} = take_dispatchable(buffer, demand)
    broadway_messages = for {:message, msg, pid, context} <- taken, do: wrap_message(msg, pid, context)

    Logger.debug("Dispatching #{length(broadway_messages)} messages, #{length(remaining)} remaining in buffer")

    state = charge_permits(%{state | buffer: remaining, demand: demand - length(broadway_messages)}, taken)

    {:noreply, broadway_messages, state}
  end

  # Permit markers follow all messages from the same broker delivery. Charge the delivery
  # only after those messages leave the buffer; a leading marker represents a delivery
  # that produced no Broadway messages.
  defp take_dispatchable(buffer, demand, taken \\ [])

  defp take_dispatchable([{:permits, _, _} = marker | rest], demand, taken) do
    take_dispatchable(rest, demand, [marker | taken])
  end

  defp take_dispatchable([{:message, _, _, _} = message | rest], demand, taken) when demand > 0 do
    take_dispatchable(rest, demand - 1, [message | taken])
  end

  defp take_dispatchable(buffer, _demand, taken), do: {Enum.reverse(taken), buffer}

  defp charge_permits(state, taken) do
    Enum.reduce(taken, state, fn
      {:permits, pid, consumed}, acc -> charge_consumer(acc, pid, consumed)
      {:message, _, _, _}, acc -> acc
    end)
  end

  # Only the charged window can cross the refill threshold; send_flow/2 is synchronous.
  defp charge_consumer(state, pid, consumed) do
    case state.consumers do
      %{^pid => {topic, outstanding}} ->
        outstanding = max(outstanding - consumed, 0)
        state = %{state | consumers: Map.put(state.consumers, pid, {topic, outstanding})}

        if outstanding <= state.flow_threshold do
          refill_consumer(state, pid, topic, outstanding)
        else
          state
        end

      _ ->
        state
    end
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

  defp refill_consumer(state, pid, topic, outstanding) do
    case Pulsar.Consumer.send_flow(pid, state.flow_refill) do
      :ok ->
        new_outstanding = outstanding + state.flow_refill

        Logger.debug(
          "Refilled flow window for #{topic}: #{state.flow_refill} permits " <>
            "(outstanding: #{outstanding} → #{new_outstanding})"
        )

        %{state | consumers: Map.put(state.consumers, pid, {topic, new_outstanding})}

      {:error, reason} ->
        # A timed-out call may still grant the permits, so retrying can over-credit the worker.
        Logger.error("Failed to refill flow window for #{topic}: #{inspect(reason)}")
        state
    end
  end

  defp schedule_health_check, do: Process.send_after(self(), :check_consumers, @health_check_interval_ms)

  # Three failures the link does not report.
  #
  # A root that is gone. The link only carries an abnormal exit, and a root stopped through
  # `Pulsar.Consumer.stop/2` exits `:normal`, which this stage ignores — so without this the
  # stage would run on believing it has a consumer that no longer exists.
  #
  # The other two leave the root itself up, so no exit signal is sent at all.
  #
  # A group with no pid stopped and was not restarted, which the client only does for a
  # failure retrying cannot fix. The worker that hit it is invisible to this stage when it
  # failed before its callback ran — a terminal subscribe error always does.
  #
  # A name that no longer resolves to the root means the client's consumer registry was
  # replaced under it: the registry links its registrants, but a root is a supervisor and
  # ignores an exit from a link that is neither its parent nor its child, so it survives
  # unregistered. Registration is not re-created, so the stage restarts to re-create it.
  defp check_consumers(state) do
    Enum.reduce_while(state.consumer_roots, :ok, fn {root, {topic, name}}, :ok ->
      cond do
        not Process.alive?(root) -> {:halt, {:gone, topic}}
        stopped_group?(root) -> {:halt, {:stopped, topic}}
        not registered?(name, root, state.client) -> {:halt, {:unregistered, topic}}
        true -> {:cont, :ok}
      end
    end)
  end

  defp registered?(name, root, client) do
    Pulsar.Client.lookup(:consumers, name, client) == {:ok, root}
  end

  defp stopped_group?(root) do
    root
    |> Supervisor.which_children()
    |> Enum.any?(fn
      {{:topic, :non_partitioned}, :undefined, _type, _modules} -> true
      {{:partition, _index}, :undefined, _type, _modules} -> true
      _child -> false
    end)
  catch
    # A root busy starting children answers on the next round, and one that exited between
    # the liveness check above and this call is caught by that check on the next round.
    :exit, _reason -> false
  end

  # Linked, so the roots go down with this stage rather than outliving it holding the
  # subscription, and this stage goes down with a root rather than idling without consumers.
  defp start_consumer!(opts) do
    case Pulsar.Consumer.start_link(opts) do
      {:ok, consumer} ->
        consumer

      other ->
        raise "failed to start Pulsar consumer for #{Keyword.fetch!(opts, :topic)}: #{inspect(other)}"
    end
  end

  defp ensure_client_running!(client) when is_atom(client) do
    if is_nil(Process.whereis(client)) do
      raise ArgumentError, """
      Pulsar client #{inspect(client)} is not running. Supervise it above the pipeline:

          children = [
            {Pulsar.Client, name: #{inspect(client)}, host: "pulsar://localhost:6650"},
            MyApp.PulsarPipeline
          ]
      """
    end
  end

  defp await_consumer_registry!(client) do
    registry = Pulsar.Client.registry(:consumers, client)

    if is_nil(await_registry(registry, @registry_wait_ms)) do
      raise ArgumentError,
            "Pulsar client #{inspect(client)} is running but its consumer registry " <>
              "#{inspect(registry)} is not; the client is still starting up or shutting down"
    end

    :ok
  end

  defp await_registry(registry, remaining_ms) do
    case Process.whereis(registry) do
      nil when remaining_ms > 0 ->
        Process.sleep(@registry_poll_ms)
        await_registry(registry, remaining_ms - @registry_poll_ms)

      found_or_nil ->
        found_or_nil
    end
  end

  defp validate_positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  defp validate_positive_integer!(value, name) do
    raise ArgumentError,
          "expected :#{name} to be a positive integer, got: #{inspect(value)}"
  end
end
