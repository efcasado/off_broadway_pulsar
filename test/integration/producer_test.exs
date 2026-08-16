defmodule OffBroadwayPulsar.Integration.ProducerTest do
  use ExUnit.Case, async: false

  alias OffBroadwayPulsar.Test.Support.DummyConsumer
  alias OffBroadwayPulsar.Test.Support.DummyPipeline
  alias OffBroadwayPulsar.Test.Support.Utils

  @moduletag :integration
  @client :producer_test_client
  @topic "persistent://public/default/broadway-integration-test"
  @topic_a "persistent://public/default/broadway-multi-topic-a"
  @topic_b "persistent://public/default/broadway-multi-topic-b"
  @partitioned_topic "persistent://public/default/broadway-failover-partitioned"
  @ownership_topic "persistent://public/default/broadway-consumer-ownership"
  @exclusive_topic "persistent://public/default/broadway-exclusive-ownership"
  @partition_count 2
  @message_count 100
  @multi_topic_message_count 20

  setup_all do
    alias OffBroadwayPulsar.Test.Support.System

    :ok = System.create_topic(@topic)
    :ok = System.create_topic(@topic_a)
    :ok = System.create_topic(@topic_b)
    :ok = System.create_partitioned_topic(@partitioned_topic, @partition_count)
    :ok = System.create_topic(@ownership_topic)
    :ok = System.create_topic(@exclusive_topic)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: "pulsar://localhost:6650"
      )

    {:ok, producer} =
      Pulsar.Producer.start_link(
        topic: @topic,
        client: @client,
        name: :test_producer
      )

    # Producer startup is asynchronous; sending before discovery finishes answers
    # {:error, :not_ready}.
    :ok = Pulsar.Producer.await_ready(producer)

    for i <- 1..@message_count do
      {:ok, _msg_id} =
        Pulsar.Producer.send(producer, "Message #{i}",
          partition_key: "key-#{rem(i, 10)}",
          ordering_key: "order-#{rem(i, 5)}",
          properties: %{"index" => "#{i}"}
        )
    end

    {:ok, producer_a} =
      Pulsar.Producer.start_link(topic: @topic_a, client: @client, name: :test_producer_a)

    {:ok, producer_b} =
      Pulsar.Producer.start_link(topic: @topic_b, client: @client, name: :test_producer_b)

    :ok = Pulsar.Producer.await_ready(producer_a)
    :ok = Pulsar.Producer.await_ready(producer_b)

    for i <- 1..@multi_topic_message_count do
      {:ok, _} = Pulsar.Producer.send(producer_a, "topic-a:#{i}")
      {:ok, _} = Pulsar.Producer.send(producer_b, "topic-b:#{i}")
    end

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "multiple Broadway producers consume messages concurrently" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "concurrency-sub",
        client: @client,
        producer_concurrency: 3,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5
      )

    producer_ids =
      for _ <- 1..@message_count do
        assert_receive {:message_handled, %Broadway.Message{acknowledger: {_, _ack_ref, producer_index}}, _processor},
                       10_000

        producer_index
      end

    unique_producers = producer_ids |> Enum.uniq() |> length()
    assert unique_producers > 1, "Expected multiple producers, got #{unique_producers}"
  end

  test "attaches to a named client" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "named-client-sub",
        client: @client,
        name: :named_client_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5
      )

    assert_receive {:message_handled, %Broadway.Message{}, _processor}, 10_000
  end

  test "messages are NACKed and redelivered" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "nack-sub",
        client: @client,
        handler: :nack,
        name: :nack_test_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5,
        consumer_opts: [
          initial_position: :earliest,
          redelivery_interval: 100
        ]
      )

    messages =
      for _ <- 1..(@message_count + 1) do
        assert_receive {:message_handled, %Broadway.Message{} = msg}, 10_000
        msg
      end

    last_message = List.last(messages)

    assert last_message.metadata.redelivery_count == 1
  end

  test "dead letter policy after max redeliveries" do
    dlq_topic = "persistent://public/default/broadway-integration-test-dlq"

    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "dlq-sub",
        client: @client,
        handler: :nack,
        name: :dlq_test_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5,
        consumer_opts: [
          initial_position: :earliest,
          redelivery_interval: 500,
          dead_letter_policy: [
            max_redelivery: 2,
            topic: dlq_topic
          ]
        ]
      )

    # :max_redelivery counts deliveries attempted before diverting, and diverting
    # replaces delivery rather than accompanying it: handled twice, never a third time.
    assert_receive {:message_handled, %Broadway.Message{data: msg_data}}, 10_000
    assert_receive {:message_handled, %Broadway.Message{data: ^msg_data}}, 5_000
    refute_receive {:message_handled, %Broadway.Message{data: ^msg_data}}, 2_000

    {:ok, _dlq_consumer} =
      Pulsar.Consumer.start(
        dlq_topic,
        "dlq-consumer",
        DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [self()]
      )

    assert_receive {:pulsar_message, ^msg_data}, 10_000
  end

  test "a wholly dead-lettered backlog keeps draining past the permit window" do
    # A diverted delivery reaches no callback, so only the flow policy can report what the
    # broker charged for it. Miscount those and the window drifts up until it never reaches
    # the threshold again, and the subscription stops with no crash and no log.
    dlq_topic = "persistent://public/default/broadway-permit-drain-dlq"
    flow_initial = 5

    {:ok, _dlq_consumer} =
      Pulsar.Consumer.start(dlq_topic, "permit-drain-dlq-consumer", DummyConsumer,
        client: @client,
        initial_position: :earliest,
        init_args: [self()]
      )

    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "permit-drain-sub",
        client: @client,
        handler: :nack,
        name: :permit_drain_pipeline,
        flow_initial: flow_initial,
        flow_threshold: 2,
        flow_refill: flow_initial,
        consumer_opts: [
          initial_position: :earliest,
          redelivery_interval: 200,
          dead_letter_policy: [max_redelivery: 1, topic: dlq_topic]
        ]
      )

    diverted =
      for _ <- 1..(flow_initial * 4) do
        assert_receive {:pulsar_message, data}, 15_000
        data
      end

    assert length(Enum.uniq(diverted)) == length(diverted)
  end

  test "message metadata is preserved" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "metadata-sub",
        client: @client,
        name: :metadata_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5
      )

    assert_receive {:message_handled, %Broadway.Message{metadata: metadata}, _processor}, 10_000

    assert metadata.topic == @topic
    assert metadata.base_topic == @topic
    assert metadata.partition == nil
    assert metadata.subscription == "metadata-sub"

    assert metadata.key =~ ~r/^key-\d$/
    assert metadata.ordering_key =~ ~r/^order-\d$/
    assert %{"index" => _} = metadata.properties
    assert is_integer(metadata.publish_time)
    assert metadata.event_time == nil
    assert metadata.redelivery_count == 0

    assert metadata.producer_name =~ ~r/^test_producer-\d+$/

    assert metadata.message_id
    assert metadata.message_id_string =~ ~r/^\d+:\d+:-?\d+/
    assert %{command: _, metadata: _} = metadata.raw
  end

  test "messages from multiple topics are all received and acknowledged" do
    total = @multi_topic_message_count * 2

    # flow_initial (5) < @multi_topic_message_count (20), so each consumer has to go
    # through several refill cycles on its own window.
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topics: [@topic_a, @topic_b],
        subscription: "multi-topic-sub",
        client: @client,
        name: :multi_topic_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5,
        consumer_opts: [initial_position: :earliest]
      )

    payloads =
      for _ <- 1..total do
        assert_receive {:message_handled, %Broadway.Message{} = msg, _processor}, 10_000
        msg.data
      end

    from_a = Enum.count(payloads, &String.starts_with?(&1, "topic-a:"))
    from_b = Enum.count(payloads, &String.starts_with?(&1, "topic-b:"))

    assert from_a == @multi_topic_message_count,
           "Expected #{@multi_topic_message_count} messages from topic-a, got #{from_a}"

    assert from_b == @multi_topic_message_count,
           "Expected #{@multi_topic_message_count} messages from topic-b, got #{from_b}"
  end

  test "pipeline graceful shutdown" do
    {:ok, broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "shutdown-sub",
        client: @client,
        name: :shutdown_test_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5
      )

    assert_receive {:message_handled, %Broadway.Message{}, _processor}, 10_000

    assert :ok = Broadway.stop(broadway)
    refute Process.alive?(broadway)
  end

  test "Failover consumers report active and passive states" do
    subscription = "failover-ownership-sub"

    {:ok, first_pipeline} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: subscription,
        client: @client,
        name: :failover_active_pipeline,
        active_state_callback: {Utils, :notify_active_state, [self(), :first]},
        consumer_opts: [subscription_type: :failover, initial_position: :latest]
      )

    assert_receive {:active_state_callback,
                    %{
                      active_state: :active,
                      subscription: ^subscription,
                      consumer_pid: consumer_pid,
                      topic: @topic
                    }, :first},
                   10_000

    assert is_pid(consumer_pid)

    {:ok, second_pipeline} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: subscription,
        client: @client,
        name: :failover_standby_pipeline,
        active_state_callback: {Utils, :notify_active_state, [self(), :second]},
        consumer_opts: [subscription_type: :failover, initial_position: :latest]
      )

    await_failover_states(%{first: :active}, subscription)

    assert :ok = Broadway.stop(first_pipeline)
    assert :ok = Broadway.stop(second_pipeline)
  end

  test "Failover callbacks identify individual partition topics" do
    subscription = "failover-partition-ownership-sub"
    callback = {Utils, :notify_active_state, [self(), :partition]}

    {:ok, pipeline} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @partitioned_topic,
        subscription: subscription,
        client: @client,
        name: :failover_partition_callback_pipeline,
        active_state_callback: callback,
        consumer_opts: [subscription_type: :failover, initial_position: :latest]
      )

    topics =
      for _ <- 1..@partition_count do
        assert_receive {:active_state_callback,
                        %{
                          active_state: :active,
                          subscription: ^subscription,
                          consumer_pid: consumer_pid,
                          topic: topic
                        }, :partition},
                       10_000

        assert is_pid(consumer_pid)
        topic
      end

    expected_topics =
      for partition <- 0..(@partition_count - 1),
          do: Pulsar.Topic.partition(@partitioned_topic, partition)

    assert MapSet.new(topics) == MapSet.new(expected_topics)
    assert :ok = Broadway.stop(pipeline)
  end

  describe "consumer ownership" do
    test "consumers go down with the pipeline that started them" do
      subscription = "ownership-shutdown-sub"

      {:ok, broadway} =
        start_pipeline(:ownership_shutdown_pipeline, @topic, subscription,
          flow_initial: 5,
          flow_threshold: 2,
          flow_refill: 5
        )

      assert_receive {:message_handled, %Broadway.Message{}, _processor}, 10_000

      assert [root] = consumer_roots(subscription)
      ref = Process.monitor(root)

      assert :ok = Broadway.stop(broadway)

      assert_receive {:DOWN, ^ref, :process, ^root, _reason}, 5_000
      assert wait_until(fn -> consumer_roots(subscription) == [] end, 5_000)
    end

    @tag :capture_log
    test "a producer-stage restart replaces its consumer root without leaving an orphan" do
      subscription = "producer-restart-sub"
      producer = start_ready_producer(:producer_restart_producer, @ownership_topic)

      {:ok, broadway} =
        start_pipeline(:producer_restart_pipeline, @ownership_topic, subscription)

      assert [root] = consumer_roots(subscription)
      root_ref = Process.monitor(root)

      [producer_name] = Broadway.producer_names(:producer_restart_pipeline)
      stage = GenServer.whereis(producer_name)
      assert is_pid(stage)
      stage_ref = Process.monitor(stage)
      Process.exit(stage, :kill)

      assert_receive {:DOWN, ^stage_ref, :process, ^stage, :killed}, 5_000
      assert_receive {:DOWN, ^root_ref, :process, ^root, _}, 5_000

      replacement_stage = wait_for_replacement_process(producer_name, stage, 5_000)
      replacement_root = wait_for_replacement_root(subscription, root, 20_000)
      assert is_pid(replacement_stage)
      assert is_pid(replacement_root)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "after restart")
      assert_receive {:message_handled, %Broadway.Message{data: "after restart"}, _processor}, 10_000

      assert :ok = Broadway.stop(broadway)
    end

    test "a normally stopped producer stage does not leave its consumer root behind" do
      subscription = "normal-producer-stop-sub"
      producer = start_ready_producer(:normal_producer_stop_producer, @ownership_topic)

      {:ok, broadway} =
        start_pipeline(:normal_producer_stop_pipeline, @ownership_topic, subscription)

      assert [root] = consumer_roots(subscription)
      root_ref = Process.monitor(root)

      [producer_name] = Broadway.producer_names(:normal_producer_stop_pipeline)
      stage = GenServer.whereis(producer_name)
      assert is_pid(stage)
      stage_ref = Process.monitor(stage)

      assert :ok = GenStage.stop(stage)
      assert_receive {:DOWN, ^stage_ref, :process, ^stage, :normal}, 5_000
      assert_receive {:DOWN, ^root_ref, :process, ^root, :normal}, 5_000

      replacement_stage = wait_for_replacement_process(producer_name, stage, 5_000)
      replacement_root = wait_for_replacement_root(subscription, root, 20_000)
      assert is_pid(replacement_stage)
      assert is_pid(replacement_root)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "after normal restart")

      assert_receive {:message_handled, %Broadway.Message{data: "after normal restart"}, _processor},
                     10_000

      assert :ok = Broadway.stop(broadway)
    end

    test "each producer stage owns its own consumers" do
      subscription = "ownership-concurrency-sub"

      {:ok, broadway} =
        start_pipeline(:ownership_concurrency_pipeline, @topic, subscription,
          producer_concurrency: 3,
          flow_initial: 5,
          flow_threshold: 2,
          flow_refill: 5
        )

      roots = consumer_roots(subscription)
      assert length(roots) == 3

      assert :ok = Broadway.stop(broadway)

      assert wait_until(fn -> consumer_roots(subscription) == [] end, 5_000)
      refute Enum.any?(roots, &Process.alive?/1)
    end

    @tag :capture_log
    test "a terminal subscription failure replaces its root once the subscription is free" do
      subscription = "exclusive-ownership-sub"
      consumer_opts = [subscription_type: :exclusive, initial_position: :earliest]
      producer = start_ready_producer(:exclusive_ownership_producer, @exclusive_topic)

      {:ok, holder} =
        start_pipeline(:exclusive_holder_pipeline, @exclusive_topic, subscription, consumer_opts: consumer_opts)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "held")
      assert_receive {:message_handled, %Broadway.Message{data: "held"}, _processor}, 10_000
      assert [holder_root] = consumer_roots(subscription)

      # :ConsumerBusy stops the group before its worker reports ready to the stage.
      {:ok, waiting} =
        start_pipeline(:exclusive_waiting_pipeline, @exclusive_topic, subscription, consumer_opts: consumer_opts)

      blocked_root = wait_for_other_root(subscription, holder_root, 5_000)
      assert is_pid(blocked_root)
      blocked_ref = Process.monitor(blocked_root)
      assert wait_until(fn -> stopped_group?(blocked_root) end, 4_000)
      holder_ref = Process.monitor(holder_root)

      assert :ok = Broadway.stop(holder)
      assert_receive {:DOWN, ^holder_ref, :process, ^holder_root, _}, 5_000
      assert_receive {:DOWN, ^blocked_ref, :process, ^blocked_root, _}, 10_000

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "free")

      assert_receive {:message_handled, %Broadway.Message{data: "free"}, _processor}, 30_000
      replacement_root = wait_for_replacement_root(subscription, blocked_root, 5_000)
      assert is_pid(replacement_root)
      assert Process.alive?(waiting)

      assert :ok = Broadway.stop(waiting)
    end

    @tag :capture_log
    test "a consumer stopped through the client is rebuilt by its stage" do
      subscription = "consumer-stop-sub"
      producer = start_ready_producer(:consumer_stop_producer, @ownership_topic)

      {:ok, broadway} =
        start_pipeline(:consumer_stop_pipeline, @ownership_topic, subscription)

      assert [root] = consumer_roots(subscription)

      # A monitor reports the normal exit that the link does not propagate.
      assert :ok = Pulsar.Consumer.stop(root, client: @client)

      replacement = wait_for_replacement_root(subscription, root, 20_000)
      assert is_pid(replacement)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "rebuilt")
      assert_receive {:message_handled, %Broadway.Message{data: "rebuilt"}, _processor}, 10_000

      assert :ok = Broadway.stop(broadway)
    end

    @tag :capture_log
    test "consumer roots survive replacement of the client's consumer Registry" do
      subscription = "registry-replacement-sub"
      producer = start_ready_producer(:ownership_producer, @ownership_topic)

      {:ok, broadway} =
        start_pipeline(:registry_replacement_pipeline, @ownership_topic, subscription)

      assert [root] = consumer_roots(subscription)
      assert :ok = Pulsar.Consumer.await_ready(root)

      registry = Pulsar.Client.registry(:consumers, @client)
      old_registry = Process.whereis(registry)
      :ok = Supervisor.stop(old_registry, :shutdown)
      replacement_registry = wait_until(fn -> Process.whereis(registry) end, 10_000)
      assert is_pid(replacement_registry)
      assert replacement_registry != old_registry
      assert Process.alive?(root)

      {:ok, _msg_id} = Pulsar.Producer.send(producer, "after")
      assert_receive {:message_handled, %Broadway.Message{data: "after"}, _processor}, 10_000

      assert :ok = Broadway.stop(broadway)
    end
  end

  defp start_pipeline(name, topic, subscription, opts \\ []) do
    defaults = [test_pid: self(), topic: topic, subscription: subscription, client: @client, name: name]
    DummyPipeline.start_link(Keyword.merge(defaults, opts))
  end

  defp start_ready_producer(name, topic) do
    {:ok, producer} = Pulsar.Producer.start_link(topic: topic, client: @client, name: name)
    :ok = Pulsar.Producer.await_ready(producer)
    producer
  end

  defp consumer_roots(subscription) do
    :consumers
    |> Pulsar.Client.registry(@client)
    |> Registry.select([{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {name, _pid} -> String.contains?(name, subscription) end)
    |> Enum.map(fn {_name, pid} -> pid end)
  rescue
    ArgumentError -> []
  end

  defp wait_until(fun, timeout) do
    do_wait_until(fun, System.monotonic_time(:millisecond) + timeout)
  end

  defp wait_for_replacement_process(name, old_pid, timeout) do
    wait_until(
      fn ->
        case GenServer.whereis(name) do
          pid when is_pid(pid) and pid != old_pid -> pid
          _other -> false
        end
      end,
      timeout
    )
  end

  defp wait_for_replacement_root(subscription, old_root, timeout) do
    wait_until(
      fn ->
        case consumer_roots(subscription) do
          [root] when root != old_root -> root
          _roots -> false
        end
      end,
      timeout
    )
  end

  defp wait_for_other_root(subscription, root, timeout) do
    wait_until(fn -> Enum.find(consumer_roots(subscription), &(&1 != root)) end, timeout)
  end

  defp stopped_group?(root) do
    root
    |> Supervisor.which_children()
    |> Enum.any?(fn {_id, pid, _type, _modules} -> pid == :undefined end)
  catch
    :exit, _reason -> false
  end

  defp do_wait_until(fun, deadline) do
    result = fun.()

    cond do
      result ->
        result

      System.monotonic_time(:millisecond) >= deadline ->
        result

      true ->
        Process.sleep(200)
        do_wait_until(fun, deadline)
    end
  end

  defp await_failover_states(states, subscription) do
    if Enum.sort(Map.values(states)) != [:active, :passive] do
      assert_receive {:active_state_callback, %{active_state: state, subscription: ^subscription, topic: @topic},
                      pipeline},
                     10_000

      await_failover_states(Map.put(states, pipeline, state), subscription)
    end
  end
end
