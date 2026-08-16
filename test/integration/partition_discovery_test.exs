defmodule OffBroadwayPulsar.Integration.PartitionDiscoveryTest do
  use ExUnit.Case, async: false

  alias OffBroadwayPulsar.Test.Support.DummyPipeline
  alias OffBroadwayPulsar.Test.Support.System
  alias OffBroadwayPulsar.Test.Support.Utils

  @moduletag :integration
  @client :partition_discovery_test_client
  @initial_partitions 2
  @expanded_partitions 4
  @discovery_interval_ms 500
  @messages_per_partition 5

  setup_all do
    {:ok, _client_pid} =
      Pulsar.Client.start_link(
        name: @client,
        host: "pulsar://localhost:6650"
      )

    on_exit(fn ->
      Pulsar.Client.stop(@client)
    end)

    :ok
  end

  test "discovers newly added partitions and consumes their messages without data loss" do
    test_id = :erlang.unique_integer([:positive])
    topic = "persistent://public/default/broadway-pd-test-#{test_id}"
    subscription = "partition-discovery-sub-#{test_id}"

    :ok = System.create_partitioned_topic(topic, @initial_partitions)

    {:ok, producer_pid} =
      Pulsar.Producer.start(topic,
        client: @client,
        name: "partition-discovery-producer-#{test_id}",
        partition_discovery_interval_ms: @discovery_interval_ms
      )

    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: self(),
        topics: [topic],
        subscription: subscription,
        client: @client,
        name: String.to_atom("partition_discovery_pipeline_#{test_id}"),
        flow_initial: 10,
        flow_threshold: 5,
        flow_refill: 5,
        consumer_opts: [
          initial_position: :earliest,
          partition_discovery_interval_ms: @discovery_interval_ms
        ]
      )

    # Wait until the producer has a group for every initial partition, confirming
    # the topic was created as partitioned and the producer is ready to route.
    assert :ok = wait_for_partition_count(producer_pid, @initial_partitions)

    initial_count = @initial_partitions * @messages_per_partition

    for i <- 1..initial_count do
      assert {:ok, _} = Pulsar.Producer.send(producer_pid, "initial-msg-#{i}", partition_key: "key-#{i}")
    end

    for _ <- 1..initial_count do
      assert_receive {:message_handled, %Broadway.Message{}, _}, 10_000
    end

    :ok = System.update_partitions(topic, @expanded_partitions)

    assert :ok = wait_for_partition_count(producer_pid, @expanded_partitions)

    new_count = @expanded_partitions * @messages_per_partition

    for i <- 1..new_count do
      assert {:ok, _} = Pulsar.Producer.send(producer_pid, "new-msg-#{i}", partition_key: "key-#{i * 100}")
    end

    new_messages =
      for _ <- 1..new_count do
        assert_receive {:message_handled, %Broadway.Message{} = msg, _}, 30_000
        msg
      end

    unique_partitions =
      new_messages
      |> Enum.map(fn msg -> msg.metadata.partition end)
      |> Enum.uniq()
      |> length()

    assert unique_partitions == @expanded_partitions,
           "Expected messages from #{@expanded_partitions} partitions, got #{unique_partitions}"
  end

  # Reaches into pulsar-elixir internals: expansion is not observable publicly, since
  # await_ready/2 is already satisfied by the partitions discovered at startup.
  defp wait_for_partition_count(producer_pid, expected) do
    Utils.wait_for(fn ->
      length(Pulsar.Topology.groups(producer_pid)) == expected
    end)
  end
end
