defmodule OffBroadway.Pulsar.Producer do
  @moduledoc """
  A Broadway producer for Apache Pulsar.

  This producer receives messages from Pulsar topics and forwards them to the
  Broadway pipeline. It implements flow control using Pulsar's permit window
  mechanism, which proactively requests batches of messages rather than
  requesting per-message.

  The connection belongs to your application: supervise a `Pulsar.Client` and this
  producer attaches its consumers to it.

  Each producer stage owns the consumers it starts. See "Consumer ownership" in
  `start_link/1`.

  See `start_link/1` for detailed configuration options.
  """

  use GenStage

  alias Broadway.Message
  alias OffBroadway.Pulsar.Consumer, as: Callback

  require Logger

  @default_consumer_opts [
    subscription_type: :shared
  ]

  # Keys the producer sets on every consumer it starts. A value given here would be silently
  # discarded by the merge below, or would break the producer's own flow accounting.
  @reserved_consumer_opts [
    client: "it comes from the producer's own :client",
    topic: "it comes from the producer's :topics",
    subscription_name: "it comes from the producer's :subscription",
    callback_module: "the producer supplies its own callback module",
    init_args: "the producer supplies its own init args",
    name: "consumer names are generated per producer stage",
    consumer_count: "use Broadway's producer: [concurrency: N]",
    flow_policy: "the producer grants refills itself",
    flow_initial: "use the producer's own :flow_initial",
    flow_threshold: "use the producer's own :flow_threshold",
    flow_refill: "use the producer's own :flow_refill"
  ]

  @reserved_keys_doc @reserved_consumer_opts |> Keyword.keys() |> Enum.map_join(", ", &"`#{inspect(&1)}`")

  @opts_schema [
    client: [
      type: :atom,
      default: :default,
      doc: """
      Name of the `Pulsar.Client` to attach to. The client must already be running; see
      "Starting a client" below.
      """
    ],
    topics: [
      type: {:custom, __MODULE__, :validate_topics, []},
      required: true,
      doc: """
      The Pulsar topics to consume from, as a non-empty list of strings. One consumer is
      started per topic.
      """
    ],
    subscription: [
      type: :string,
      required: true,
      doc: "The subscription name."
    ],
    active_state_callback: [
      type: {:or, [:mfa, {:in, [nil]}]},
      default: nil,
      doc: """
      A `{module, function, extra_args}` tuple that receives active/passive state changes for
      Failover consumers. `nil` disables it. See "Failover Active State" below.
      """
    ],
    consumer_opts: [
      type: :keyword_list,
      default: @default_consumer_opts,
      doc: """
      Options forwarded to `Pulsar.Consumer.start_link/1`, applied to every topic. They are
      documented and validated by `Pulsar.Consumer`, which rejects unknown keys. Setting this
      replaces the default rather than merging into it.

      The keys the producer sets itself are rejected here: #{@reserved_keys_doc}. Use
      `producer: [concurrency: N]` for the consumer count and the `:flow_*` options above for
      flow control.

      `:batch_index_ack_enabled` is worth enabling for Broadway, which routinely completes
      messages from one batch out of order, but it requires
      `acknowledgmentAtBatchIndexLevelEnabled=true` on the broker.
      """
    ],
    flow_initial: [
      type: :pos_integer,
      default: 100,
      doc: "Permits each consumer grants when it subscribes."
    ],
    flow_threshold: [
      type: :pos_integer,
      default: 50,
      doc: "Refill once outstanding permits reach this level. Must be less than `:flow_initial`."
    ],
    flow_refill: [
      type: :pos_integer,
      default: 50,
      doc: "Permits granted by each refill."
    ]
  ]

  # Terminal subscription errors can stop a group without stopping its root.
  @health_check_interval_ms 5_000

  @doc """
  Starts an `OffBroadway.Pulsar` producer process linked to the current
  process.

  ## Configuration

  #{NimbleOptions.docs(@opts_schema)}

  Options are validated when the stage starts, so a misconfigured pipeline fails at boot
  rather than at the first message.

  ## Flow Control

  Two budgets bound the pipeline, and they are not shaped the same way. Each consumer holds its
  own permit window, bounding what the broker delivers into the producer's buffer. Demand is a
  single per-stage counter: the stage keeps one buffer and one demand tally across all of its
  consumers, and dispatches in arrival order regardless of which topic or partition a delivery
  came from. Deliveries that arrive ahead of demand wait in that shared buffer.

  The `:flow_*` options replace the consumer's own automatic refills: the producer sets the consumer's
  `:flow_policy` to report what each delivery cost and grants the refills itself, sized to
  what Broadway has taken. Each worker still grants its full `:flow_initial` window when it
  subscribes, independently of pipeline demand. Processor demand is served from the window
  already granted, rather than triggering a flow request of its own. Permits are charged when
  messages leave the buffer for the processors, not when they are acknowledged, so a slow
  `c:Broadway.handle_message/3` does not hold the window closed. Because the buffer is shared,
  a delivery is charged only once it reaches the head, so one consumer's refill can wait behind
  another's undispatched messages; when demand is chronically short, the windows drain together
  rather than independently.

  Each consumer keeps its own window — one per topic, and one per partition of a
  partitioned topic. With `producer: [concurrency: N]`, each producer has its own
  consumers, and so its own windows. A refill is granted once the window falls to
  `:flow_threshold` and adds `:flow_refill` on top of what is left, so a consumer's window peaks
  at `max(flow_initial, flow_threshold + flow_refill)` and a stage's read-ahead is roughly that
  times its consumer count. The defaults make the two halves equal, at 100 permits; raising
  `:flow_refill` alone raises the ceiling well above `:flow_initial`.

  Size that total against the pipeline's in-flight demand, bounded above by `max_demand` ×
  processor concurrency (or `batch_size` when batching). Treat that as a ceiling rather than a
  steady figure: GenStage reissues demand in `max_demand - min_demand` increments once a stage's
  in-flight events fall to `:min_demand`, so outstanding demand oscillates between the two
  instead of sitting at the maximum. With Broadway's default `:min_demand` of half `:max_demand`,
  the average settles nearer three quarters of the ceiling — size to that order of magnitude, not
  to an exact number.

  A window smaller than that demand bounds ingress rate, not concurrency. Because permits are
  charged at dispatch, each refill admits another delivery while earlier messages are still being
  processed, so successive refills can fill all outstanding demand; sustained throughput is
  roughly the window divided by the refill round-trip, and processors idle only once that falls
  below what they can consume. Far above the demand, messages accumulate unacknowledged in the
  producer's buffer, costing memory and counting against the broker's
  `maxUnackedMessagesPerConsumer` limit, which stops delivery on shared subscriptions once
  crossed.

  ## Consumer ownership

  Each producer stage starts one linked consumer root per topic. Pulsar supervises the
  partition groups and workers below each root; Broadway supervises the stage. A root exit
  makes Broadway restart the stage and recreate all of its roots, while stopping the stage
  stops its linked roots. Retryable worker failures remain local to the Pulsar topology.

  Consumer roots use the client's broker infrastructure but are not children of its consumer
  `DynamicSupervisor`. Consequently, `Pulsar.Client.consumers/1` does not list them. The stage
  owns them by pid, so replacing the client's consumer Registry does not affect them, although
  their former names no longer resolve. Stop the Broadway pipeline to stop them permanently.

  A terminal subscription error can leave a root alive with a stopped group. The stage
  detects that state and restarts instead of remaining healthy with nothing to consume.

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
          topics: ["my-topic"],
          subscription: "my-subscription"
        }
      ]
  """
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    opts = validate_options!(opts)

    topics = Keyword.fetch!(opts, :topics)
    subscription = Keyword.fetch!(opts, :subscription)
    client = Keyword.fetch!(opts, :client)

    active_state_callback = Keyword.fetch!(opts, :active_state_callback)

    ensure_client_running!(client)

    flow_initial = Keyword.fetch!(opts, :flow_initial)
    flow_threshold = Keyword.fetch!(opts, :flow_threshold)
    flow_refill = Keyword.fetch!(opts, :flow_refill)

    if flow_threshold >= flow_initial do
      raise ArgumentError,
            "flow_threshold (#{flow_threshold}) must be less than flow_initial (#{flow_initial})"
    end

    # Each worker grants its own initial window on subscribe, so restarts and
    # late-discovered partitions come back with permits rather than waiting to be given
    # any. Refills are this producer's, driven by what Broadway takes.
    consumer_opts_base =
      opts
      |> Keyword.fetch!(:consumer_opts)
      |> Keyword.merge(
        client: client,
        flow_initial: flow_initial,
        flow_policy: {Callback, :report_permits, [self()]}
      )

    # One linked root per topic; unique names also separate concurrent producer stages.
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

        root = start_consumer!(topic_opts)
        {root, {topic, Process.monitor(root)}}
      end)

    state = %{
      # consumer_pid => {topic, outstanding_permits}, populated via :consumer_ready.
      # Permits belong to a worker instance, so the worker is the key; the topic only
      # rides along for logging.
      consumers: %{},
      # root pid => {topic, monitor_ref}
      consumer_roots: consumer_roots,
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

  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    case state.consumer_roots do
      %{^pid => {topic, ^monitor_ref}} ->
        Logger.error("Consumer root for #{topic} exited: #{inspect(reason)}; stopping")

        {:stop, {:shutdown, {:consumer_gone, topic}}, state}

      _worker_or_unknown ->
        forget_consumer(pid, state)
    end
  end

  def handle_info(:check_consumers, state) do
    case check_consumers(state) do
      :ok ->
        schedule_health_check()
        {:noreply, [], state}

      {:stopped, topic} ->
        Logger.error("Consumer for #{topic} has a stopped group and no worker left; stopping")

        {:stop, {:shutdown, {:consumer_stopped, topic}}, state}
    end
  end

  @impl GenStage
  def terminate(_reason, state) do
    Enum.each(state.consumer_roots, fn {root, _metadata} ->
      Pulsar.Consumer.stop(root)
    end)
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

  # Terminal subscription errors can stop a group without stopping its root.
  defp check_consumers(state) do
    Enum.reduce_while(state.consumer_roots, :ok, fn {root, {topic, _monitor_ref}}, :ok ->
      if stopped_group?(root), do: {:halt, {:stopped, topic}}, else: {:cont, :ok}
    end)
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
    # Root exits are reported by its monitor.
    :exit, _reason -> false
  end

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

  defp validate_options!(opts) do
    # Broadway injects :broadway (pipeline name and stage index) into the producer's options.
    {_broadway, opts} = Keyword.pop(opts, :broadway)

    opts =
      case NimbleOptions.validate(opts, @opts_schema) do
        {:ok, opts} -> opts
        {:error, error} -> raise ArgumentError, Exception.message(error)
      end

    consumer_opts = Keyword.fetch!(opts, :consumer_opts)

    Enum.each(@reserved_consumer_opts, fn {key, reason} ->
      if Keyword.has_key?(consumer_opts, key) do
        raise ArgumentError, "invalid value for :consumer_opts: #{inspect(key)} cannot be set here, #{reason}"
      end
    end)

    opts
  end

  # An empty list would leave the stage running with nothing to consume, so it is rejected
  # here rather than by the {:list, :string} type.
  @doc false
  @spec validate_topics(term()) :: {:ok, [String.t()]} | {:error, String.t()}
  def validate_topics([_ | _] = topics) do
    if Enum.all?(topics, &is_binary/1),
      do: {:ok, topics},
      else: {:error, "expected a non-empty list of topic names, got: #{inspect(topics)}"}
  end

  def validate_topics(other) do
    {:error, "expected a non-empty list of topic names, got: #{inspect(other)}"}
  end
end
