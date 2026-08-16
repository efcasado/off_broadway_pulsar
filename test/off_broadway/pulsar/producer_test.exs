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
      # validated later, this would fail on the client's consumer registry being absent
      # instead, which says nothing about the option that is actually wrong.
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

    test "removes a restarted worker without removing its same-topic replacement" do
      wait = fn ->
        receive do
          :stop -> :ok
        end
      end

      old_worker = spawn(wait)
      replacement = spawn(wait)

      on_exit(fn ->
        send(old_worker, :stop)
        send(replacement, :stop)
      end)

      assert {:noreply, [], state} =
               Producer.handle_info({:consumer_ready, old_worker, @context}, %{state() | consumers: %{}})

      assert {:noreply, [], state} =
               Producer.handle_info({:consumer_ready, replacement, @context}, state)

      Process.exit(old_worker, :kill)
      assert_receive {:DOWN, ref, :process, ^old_worker, :killed}

      assert {:noreply, [], state} =
               Producer.handle_info({:DOWN, ref, :process, old_worker, :killed}, state)

      assert state.consumers == %{replacement => {"topic", 10}}
    end

    test "drops the dead worker's buffered entries and leaves the rest" do
      {:ok, alive} = start_supervised({StubConsumer, self()})
      dead = self()

      buffer = [
        {:message, @message, dead, @context},
        {:message, @message, alive, @context},
        {:permits, dead, 2}
      ]

      state = %{state() | buffer: buffer, demand: 0, consumers: %{dead => {"topic", 10}, alive => {"other", 10}}}

      assert {:noreply, [], new_state} =
               Producer.handle_info({:DOWN, make_ref(), :process, dead, :killed}, state)

      assert new_state.buffer == [{:message, @message, alive, @context}]
      refute Map.has_key?(new_state.consumers, dead)
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
      # One delivery: two messages, then the marker saying what the broker charged for it.
      buffer = [
        {:message, @message, self(), @context},
        {:message, @message, self(), @context},
        {:permits, self(), 2}
      ]

      pid = self()

      assert {:noreply, [_first], held} = Producer.handle_demand(1, %{state() | buffer: buffer, demand: 0})
      assert %{^pid => {"topic", 10}} = held.consumers
      assert [{:message, _, _, _}, {:permits, _, 2}] = held.buffer

      assert {:noreply, [_second], charged} = Producer.handle_demand(1, held)
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

      assert_receive {:send_flow, 5}
      assert %{^consumer => {"topic", 7}} = new_state.consumers
    end

    test "leaves a consumer that was not charged alone, however low its window" do
      {:ok, charged} = start_supervised({StubConsumer, self()}, id: :charged)
      {:ok, idle} = start_supervised({StubConsumer, self()}, id: :idle)

      state = %{
        state()
        | consumers: %{charged => {"charged", 3}, idle => {"idle", 0}},
          flow_threshold: 2,
          flow_refill: 5
      }

      assert {:noreply, [], new_state} = Producer.handle_info({:permits_consumed, charged, 1}, state)

      assert_receive {:send_flow, 5}
      refute_receive {:send_flow, _}
      assert %{^idle => {"idle", 0}} = new_state.consumers
    end
  end

  describe "consumer ownership" do
    test "stops when a known worker fails on an error the client will not retry" do
      reason = {:ConsumerBusy, "Exclusive consumer is already connected"}

      assert {:stop, {:shutdown, {:consumer_terminal_error, "topic", ^reason}}, _state} =
               Producer.handle_info({:DOWN, make_ref(), :process, self(), {:shutdown, reason}}, state())
    end

    test "keeps running when a worker it does not know stops terminally" do
      state = %{state() | consumers: %{}}

      assert {:noreply, [], new_state} =
               Producer.handle_info(
                 {:DOWN, make_ref(), :process, self(), {:shutdown, {:TopicNotFound, "gone"}}},
                 state
               )

      assert new_state.consumers == %{}
    end

    test "keeps running when a worker crashes for a reason the client retries" do
      assert {:noreply, [], new_state} =
               Producer.handle_info({:DOWN, make_ref(), :process, self(), :broker_crashed}, state())

      assert new_state.consumers == %{}
    end

    test "the health check leaves a registered consumer whose groups are all running alone" do
      %{root: root, state: state} = healthy_consumer()

      assert {:noreply, [], _state} = Producer.handle_info(:check_consumers, state)
      assert Process.alive?(root)
    end

    test "the health check stops when a consumer has a group the client gave up on" do
      %{root: root, state: state} = healthy_consumer()

      # What a worker stopping with {:shutdown, terminal_reason} leaves behind: a group that
      # exited cleanly and a root that keeps the child registered without a pid.
      [{_id, group, _type, _modules}] = Supervisor.which_children(root)
      :ok = Agent.stop(group)

      assert {:stop, {:shutdown, {:consumer_stopped, "topic"}}, _state} =
               Producer.handle_info(:check_consumers, state)
    end

    test "the health check stops when the consumer's registration is no longer its own" do
      %{state: state} = healthy_consumer()

      # A replaced registry comes back under the same name and without the entries: the root
      # is a supervisor, so it survives the registry it was linked to and never re-registers.
      :ok = stop_supervised!(:registry)

      start_supervised!({Registry, keys: :unique, name: Pulsar.Client.registry(:consumers, :health_client)},
        id: :registry
      )

      assert {:stop, {:shutdown, {:consumer_unregistered, "topic"}}, _state} =
               Producer.handle_info(:check_consumers, state)
    end

    test "the health check stops when a root is gone, however it went" do
      %{state: state} = healthy_consumer()

      # Pulsar.Consumer.stop/2 ends at Supervisor.stop/1, which exits :normal — a signal a
      # stage that does not trap exits ignores, so the link never answers for this one.
      :ok = stop_supervised!(:root)

      assert {:stop, {:shutdown, {:consumer_gone, "topic"}}, _state} =
               Producer.handle_info(:check_consumers, state)
    end
  end

  # A registered consumer root as Pulsar builds one: a supervisor named through the client's
  # consumer registry, holding one group child per topic or partition.
  defp healthy_consumer do
    client = :health_client
    registry = Pulsar.Client.registry(:consumers, client)
    start_supervised!({Registry, keys: :unique, name: registry}, id: :registry)

    root = start_supervised!(consumer_root_spec(registry, "topic-sub-1"))

    %{root: root, state: %{state() | consumer_roots: %{root => {"topic", "topic-sub-1"}}, client: client}}
  end

  defp consumer_root_spec(registry, name) do
    group = %{
      id: {:topic, :non_partitioned},
      start: {Agent, :start_link, [fn -> :ok end]},
      restart: :transient
    }

    %{
      id: :root,
      start: {Supervisor, :start_link, [[group], [strategy: :one_for_one, name: {:via, Registry, {registry, name}}]]},
      type: :supervisor
    }
  end

  defp state do
    %{
      consumers: %{self() => {"topic", 10}},
      consumer_roots: %{},
      client: :default,
      demand: 10,
      buffer: [],
      flow_initial: 10,
      flow_threshold: 2,
      flow_refill: 5
    }
  end
end
