defmodule OffBroadwayPulsar.Integration.ProducerTest do
  use ExUnit.Case, async: false

  alias OffBroadwayPulsar.Test.Support.DummyConsumer
  alias OffBroadwayPulsar.Test.Support.DummyPipeline

  @moduletag :integration
  @client :producer_test_client
  @topic "persistent://public/default/broadway-integration-test"
  @topic_a "persistent://public/default/broadway-multi-topic-a"
  @topic_b "persistent://public/default/broadway-multi-topic-b"
  @message_count 100
  @multi_topic_message_count 20

  setup_all do
    alias OffBroadwayPulsar.Test.Support.System

    :ok = System.create_topic(@topic)
    :ok = System.create_topic(@topic_a)
    :ok = System.create_topic(@topic_b)

    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: "pulsar://localhost:6650"
      )

    {:ok, producer} =
      Pulsar.Producer.start_link(@topic,
        client: @client,
        name: :test_producer
      )

    # Produce messages with partition_key and ordering_key
    for i <- 1..@message_count do
      {:ok, _msg_id} =
        Pulsar.Producer.send_message(producer, "Message #{i}",
          partition_key: "key-#{rem(i, 10)}",
          ordering_key: "order-#{rem(i, 5)}",
          properties: %{"index" => "#{i}"}
        )
    end

    # Produce messages to the two multi-topic test topics.
    # Each message payload encodes its source topic for assertion purposes.
    {:ok, producer_a} =
      Pulsar.Producer.start_link(@topic_a, client: @client, name: :test_producer_a)

    {:ok, producer_b} =
      Pulsar.Producer.start_link(@topic_b, client: @client, name: :test_producer_b)

    for i <- 1..@multi_topic_message_count do
      {:ok, _} = Pulsar.Producer.send_message(producer_a, "topic-a:#{i}")
      {:ok, _} = Pulsar.Producer.send_message(producer_b, "topic-b:#{i}")
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

  test "application-managed client pattern" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "app-managed-sub",
        client: @client,
        name: :app_managed_pipeline,
        flow_initial: 5,
        flow_threshold: 2,
        flow_refill: 5
      )

    assert_receive {:message_handled, %Broadway.Message{}, _processor}, 10_000
  end

  test "producer-managed client pattern" do
    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: "producer-managed-sub",
        host: "pulsar://localhost:6650",
        name: :producer_managed_pipeline,
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

    assert last_message.metadata.command.redelivery_count == 1
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

    assert_receive {:message_handled, %Broadway.Message{data: msg_data}}, 10_000
    assert_receive {:message_handled, %Broadway.Message{data: ^msg_data}}, 5_000
    assert_receive {:message_handled, %Broadway.Message{data: ^msg_data}}, 5_000

    {:ok, _dlq_consumer} =
      Pulsar.start_consumer(
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

    pulsar_metadata = metadata.metadata
    assert pulsar_metadata.partition_key
    assert pulsar_metadata.producer_name == "test_producer"

    assert Enum.any?(pulsar_metadata.properties, fn prop -> prop.key == "index" end)
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

  test "Failover consumers report active state through the callback" do
    subscription = "failover-ownership-sub"
    callback = {__MODULE__, :notify_active_state, [self()]}

    {:ok, pipeline} =
      DummyPipeline.start_link(
        test_pid: self(),
        topic: @topic,
        subscription: subscription,
        client: @client,
        name: :failover_callback_pipeline,
        active_state_callback: callback,
        consumer_opts: [subscription_type: :Failover, initial_position: :latest]
      )

    assert_receive {:active_state_callback,
                    %{
                      active_state: :active,
                      subscription: ^subscription,
                      consumer_pid: consumer_pid,
                      topic: @topic
                    }},
                   10_000

    assert is_pid(consumer_pid)
    assert :ok = Broadway.stop(pipeline)
  end

  def notify_active_state(metadata, test_pid) do
    send(test_pid, {:active_state_callback, metadata})
  end
end
