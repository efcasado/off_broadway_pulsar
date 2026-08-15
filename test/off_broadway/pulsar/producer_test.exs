defmodule OffBroadway.Pulsar.ProducerTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias OffBroadway.Pulsar.Producer
  alias OffBroadwayPulsar.Test.Support.StubConsumer

  @context %{
    topic: "topic",
    base_topic: "topic",
    partition: nil,
    subscription_name: "sub",
    subscription_type: :shared,
    consumer_name: "topic-sub-0"
  }

  @message %Pulsar.Message{
    payload: "ok",
    message_id: %{ledgerId: 1, entryId: 2, partition: -1, batch_index: -1},
    raw: %{command: %{redelivery_count: 0}}
  }

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

    test "validates flow options before starting any consumer" do
      # A registered name is all ensure_client_running!/1 checks. Were the flow options
      # validated after the consumers are started, this would fail with a MatchError on
      # Pulsar.Consumer.start/1's {:error, :client_not_found} instead.
      start_supervised!(%{
        id: :fake_client,
        start: {Agent, :start_link, [fn -> :ok end, [name: :fake_client]]}
      })

      assert_raise ArgumentError, ~r/flow_threshold \(10\) must be less than flow_initial \(10\)/, fn ->
        Producer.init(topic: "t", subscription: "s", client: :fake_client, flow_initial: 10, flow_threshold: 10)
      end
    end
  end

  describe ":consumer_ready" do
    test "registers the worker with the window it granted itself" do
      assert {:noreply, [], new_state} =
               Producer.handle_info({:consumer_ready, self(), @context}, %{state() | consumers: %{}})

      pid = self()
      assert %{^pid => {"topic", 10}} = new_state.consumers
    end

    test "forgets a worker that goes down, so no refill is sent to it" do
      assert {:noreply, [], new_state} =
               Producer.handle_info({:DOWN, make_ref(), :process, self(), :killed}, state())

      assert new_state.consumers == %{}
    end
  end

  describe "dispatch" do
    test "wraps a message and leaves it for the acknowledger" do
      assert {:noreply, [%Message{data: "ok", metadata: metadata}], new_state} =
               Producer.handle_info({:pulsar_message, @message, self(), @context}, state())

      assert metadata.message_id_string == "1:2:-1"
      assert metadata.topic == "topic"
      assert new_state.buffer == []
      # Nothing is charged until the flow policy reports what the delivery cost.
      pid = self()
      assert %{^pid => {"topic", 10}} = new_state.consumers
    end

    test "dispatches up to demand and buffers the rest" do
      buffer = for _ <- 1..3, do: {:message, @message, self(), @context}
      state = %{state() | buffer: buffer, demand: 0}

      assert {:noreply, dispatched, new_state} = Producer.handle_demand(2, state)

      assert length(dispatched) == 2
      assert length(new_state.buffer) == 1
      assert new_state.demand == 0
    end
  end

  describe ":permits_consumed" do
    test "charges the window only once Broadway has taken the delivery's messages" do
      state = %{state() | demand: 1}

      # Two messages and the marker for the delivery that carried them, but demand for one.
      buffer = [
        {:message, @message, self(), @context},
        {:message, @message, self(), @context},
        {:permits, self(), 2}
      ]

      assert {:noreply, [_one], held} = Producer.handle_demand(0, %{state | buffer: buffer})

      pid = self()
      assert %{^pid => {"topic", 10}} = held.consumers
      assert [{:message, _, _, _}, {:permits, _, 2}] = held.buffer

      assert {:noreply, [_two], charged} = Producer.handle_demand(1, held)

      assert %{^pid => {"topic", 8}} = charged.consumers
      assert charged.buffer == []
    end

    test "charges a delivery no callback saw without waiting for demand" do
      state = %{state() | demand: 0}

      assert {:noreply, [], new_state} = Producer.handle_info({:permits_consumed, self(), 2}, state)

      pid = self()
      assert %{^pid => {"topic", 8}} = new_state.consumers
      assert new_state.buffer == []
    end

    test "refills once the window falls to the threshold" do
      {:ok, consumer} = start_supervised({StubConsumer, self()})
      state = %{state() | consumers: %{consumer => {"topic", 3}}, flow_threshold: 2, flow_refill: 5}

      assert {:noreply, [], new_state} = Producer.handle_info({:permits_consumed, consumer, 1}, state)

      # 3 - 1 = 2, at the threshold, so a refill of 5 follows.
      assert_receive {:send_flow, 5}
      assert %{^consumer => {"topic", 7}} = new_state.consumers
    end
  end

  defp state do
    %{
      consumers: %{self() => {"topic", 10}},
      demand: 10,
      buffer: [],
      flow_initial: 10,
      # Below the outstanding count, so no refill fires and a test observes the
      # decrement rather than a refill on top of it.
      flow_threshold: 2,
      flow_refill: 5
    }
  end
end
