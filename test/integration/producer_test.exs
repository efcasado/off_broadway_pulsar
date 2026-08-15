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
  @partition_count 2
  @message_count 100
  @multi_topic_message_count 20

  setup_all do
    alias OffBroadwayPulsar.Test.Support.System

    :ok = System.create_topic(@topic)
    :ok = System.create_topic(@topic_a)
    :ok = System.create_topic(@topic_b)
    :ok = System.create_partitioned_topic(@partitioned_topic, @partition_count)

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

    # Produce messages with partition_key and ordering_key
    for i <- 1..@message_count do
      {:ok, _msg_id} =
        Pulsar.Producer.send(producer, "Message #{i}",
          partition_key: "key-#{rem(i, 10)}",
          ordering_key: "order-#{rem(i, 5)}",
          properties: %{"index" => "#{i}"}
        )
    end

    # Produce messages to the two multi-topic test topics.
    # Each message payload encodes its source topic for assertion purposes.
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

    # @topic is not partitioned, so :topic and :base_topic agree and there is no index.
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

    # Workers are named after their group and their position in it.
    assert metadata.producer_name =~ ~r/^test_producer-\d+$/

    assert metadata.message_id
    assert metadata.message_id_string =~ ~r/^\d+:\d+:-?\d+/
    assert %{command: _, metadata: _} = metadata.raw
  end

  test "messages from multiple topics are all received and acknowledged" do
    total = @multi_topic_message_count * 2

    # flow_initial (5) < @multi_topic_message_count (20) forces each consumer
    # to go through multiple refill cycles, exercising the per-consumer flow
    # tracking in maybe_refill_flow/refill_consumer.
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

  defp await_failover_states(states, subscription) do
    if Enum.sort(Map.values(states)) != [:active, :passive] do
      assert_receive {:active_state_callback, %{active_state: state, subscription: ^subscription, topic: @topic},
                      pipeline},
                     10_000

      await_failover_states(Map.put(states, pipeline, state), subscription)
    end
  end
end
