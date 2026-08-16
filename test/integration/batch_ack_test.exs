defmodule OffBroadwayPulsar.Integration.BatchAckTest do
  use ExUnit.Case, async: false

  alias OffBroadwayPulsar.Test.Support.DummyPipeline
  alias OffBroadwayPulsar.Test.Support.System

  @moduletag :integration
  @client :batch_ack_test_client
  @topic "persistent://public/default/broadway-batch-index-ack"
  @batch ["msg-1", "msg-2", "msg-3"]
  @failing "msg-2"

  @index_topic @topic <> "-index"
  @entry_topic @topic <> "-entry"

  setup_all do
    :ok = System.create_topic(@index_topic)
    :ok = System.create_topic(@entry_topic)

    {:ok, _client_pid} = Pulsar.Client.start_link(name: @client, host: "pulsar://localhost:6650")

    :ok
  end

  test "a failed message is redelivered without its batch siblings" do
    produce_one_batch(@index_topic, :batch_index_ack_producer)

    deliveries = run_pipeline(@index_topic, "batch-index-ack-sub", batch_index_ack_enabled: true)

    assert_one_entry(deliveries)

    assert tally(deliveries) == %{"msg-1" => 1, @failing => 2, "msg-3" => 1}
  end

  test "without batch index acking the whole entry is redelivered" do
    produce_one_batch(@entry_topic, :entry_ack_producer)

    deliveries = run_pipeline(@entry_topic, "entry-ack-sub", [])

    assert_one_entry(deliveries)

    assert tally(deliveries) == %{"msg-1" => 2, @failing => 2, "msg-3" => 2}
  end

  defp produce_one_batch(topic, name) do
    {:ok, producer} =
      Pulsar.Producer.start_link(
        topic: topic,
        client: @client,
        name: name,
        batch_enabled: true,
        # Filling the batch is what flushes it, so the messages land in one entry.
        batch_size: length(@batch),
        flush_interval: 60_000
      )

    :ok = Pulsar.Producer.await_ready(producer)

    # Pulsar.Producer.send/3 waits for the receipt, so a blocking caller would sit in a batch
    # it never sends the rest of.
    refs =
      for payload <- @batch do
        {:ok, ref} = Pulsar.Producer.send_async(producer, payload)
        ref
      end

    for ref <- refs, do: {:ok, _message_id} = Pulsar.Producer.await(ref, 10_000)

    :ok = Pulsar.Producer.stop(producer, client: @client)
  end

  defp run_pipeline(topic, subscription, extra_consumer_opts) do
    test_pid = self()
    {:ok, failed_once} = Agent.start_link(fn -> false end)

    handler = fn message, _context ->
      send(test_pid, {:handled, message.data, message.metadata.message_id_string})

      if message.data == @failing and not Agent.get_and_update(failed_once, &{&1, true}) do
        Broadway.Message.failed(message, "first attempt fails")
      else
        message
      end
    end

    {:ok, _broadway} =
      DummyPipeline.start_link(
        test_pid: test_pid,
        topics: [topic],
        subscription: subscription,
        client: @client,
        handler: handler,
        name: :"#{subscription}_pipeline",
        consumer_opts:
          Keyword.merge(
            [initial_position: :earliest, redelivery_interval: 500],
            extra_consumer_opts
          )
      )

    collect_deliveries()
  end

  defp collect_deliveries(acc \\ []) do
    receive do
      {:handled, payload, message_id} -> collect_deliveries([{payload, message_id} | acc])
    after
      3_000 -> Enum.reverse(acc)
    end
  end

  defp tally(deliveries) do
    deliveries
    |> Enum.map(fn {payload, _message_id} -> payload end)
    |> Enum.frequencies()
  end

  # A test that silently stopped batching would prove nothing.
  defp assert_one_entry(deliveries) do
    ids = deliveries |> Enum.map(fn {_payload, message_id} -> message_id end) |> Enum.uniq()

    entries =
      ids
      |> Enum.map(fn id -> id |> String.split(":") |> Enum.take(2) end)
      |> Enum.uniq()

    assert length(entries) == 1, "expected one batched entry, got ids: #{inspect(ids)}"

    batch_indexes =
      ids
      |> Enum.map(fn id -> id |> String.split(":") |> List.last() end)
      |> Enum.sort()

    assert batch_indexes == ["0", "1", "2"], "expected batch indexes 0..2, got ids: #{inspect(ids)}"
  end
end
