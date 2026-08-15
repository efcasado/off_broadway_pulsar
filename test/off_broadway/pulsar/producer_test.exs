defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias OffBroadway.Pulsar.Producer
  alias OffBroadwayPulsar.Test.Support.StubConsumer

  describe "init/1" do
    test "raises a helpful error when the client is not running" do
      assert_raise ArgumentError, ~r/Pulsar client :no_such_client is not running/, fn ->
        Producer.init(topic: "t", subscription: "s", client: :no_such_client)
      end
    end

    test "raises when neither :topic nor :topics is given" do
      assert_raise ArgumentError, "either :topic or :topics is required", fn ->
        Producer.init(subscription: "s")
      end
    end
  end

  describe "undeliverable messages" do
    setup do
      {:ok, consumer} = start_supervised({StubConsumer, self()})
      %{consumer: consumer, state: state(consumer)}
    end

    test "an incomplete chunked message is dropped, acked and counted", %{consumer: consumer, state: state} do
      # Two of three chunks arrived before it expired, so the broker charged two permits.
      message = %Pulsar.Message{
        payload: "part",
        message_id: [:id_1, :id_2],
        chunk_metadata: %{chunked: true, complete: false, message_ids: [:id_1, :id_2]}
      }

      assert {:noreply, [], new_state} = deliver(message, state)

      assert_receive {:ack, [:id_1, :id_2]}
      assert new_state.buffer == []
      assert %{"topic" => {^consumer, 8}} = new_state.consumers
    end

    test "an invalid message is dropped, acked and counted", %{consumer: consumer, state: state} do
      message = %Pulsar.Message{payload: "corrupt", message_id: :id, validation_error: :checksum_mismatch}

      assert {:noreply, [], new_state} = deliver(message, state)

      assert_receive {:ack, [:id]}
      assert new_state.buffer == []
      assert %{"topic" => {^consumer, 9}} = new_state.consumers
    end

    test "a complete, valid message is dispatched and left for the acknowledger", %{state: state} do
      message_id = %{ledgerId: 1, entryId: 2, partition: -1, batch_index: -1}
      message = %Pulsar.Message{payload: "ok", message_id: message_id, raw: %{command: %{redelivery_count: 0}}}

      assert {:noreply, [%Message{data: "ok", metadata: metadata}], new_state} = deliver(message, state)

      assert metadata.message_id_string == "1:2:-1"
      refute_receive {:ack, _}
      assert new_state.buffer == []
    end
  end

  defp deliver(message, state) do
    %{"topic" => {consumer, _permits}} = state.consumers
    Producer.handle_info({:pulsar_message, message, consumer, context()}, state)
  end

  defp state(consumer) do
    %{
      consumers: %{"topic" => {consumer, 10}},
      demand: 10,
      buffer: [],
      flow_initial: 10,
      # Below the outstanding count, so no refill fires and the test observes the
      # decrement rather than a refill on top of it.
      flow_threshold: 2,
      flow_refill: 5
    }
  end

  defp context do
    %{
      topic: "topic",
      base_topic: "topic",
      partition: nil,
      subscription_name: "sub",
      subscription_type: :shared,
      consumer_name: "topic-sub-0"
    }
  end
end
