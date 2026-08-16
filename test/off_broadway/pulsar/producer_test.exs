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
        Producer.init(topics: ["t"], subscription: "s", client: :no_such_client)
      end
    end

    test "raises when :topics is not given" do
      assert_raise ArgumentError, ~r/required :topics option not found/, fn ->
        Producer.init(subscription: "s")
      end
    end

    test "raises when :topics is empty" do
      assert_raise ArgumentError, ~r/expected a non-empty list of topic names, got: \[\]/, fn ->
        Producer.init(topics: [], subscription: "s")
      end
    end

    test "raises when :topics is not a list of strings" do
      assert_raise ArgumentError, ~r/expected a non-empty list of topic names, got: "t"/, fn ->
        Producer.init(topics: "t", subscription: "s")
      end
    end

    test "raises when :subscription is missing" do
      assert_raise ArgumentError, ~r/required :subscription option not found/, fn ->
        Producer.init(topics: ["t"])
      end
    end

    test "raises on an option of the wrong type" do
      assert_raise ArgumentError, ~r/invalid value for :flow_initial option/, fn ->
        Producer.init(topics: ["t"], subscription: "s", flow_initial: "100")
      end
    end

    test "raises on a malformed :active_state_callback" do
      assert_raise ArgumentError, ~r/invalid value for :active_state_callback option/, fn ->
        Producer.init(topics: ["t"], subscription: "s", active_state_callback: {SomeModule, :handle})
      end
    end

    test "accepts an explicit nil :active_state_callback, as when it comes from application env" do
      assert_raise ArgumentError, ~r/Pulsar client :no_such_client is not running/, fn ->
        Producer.init(topics: ["t"], subscription: "s", client: :no_such_client, active_state_callback: nil)
      end
    end

    test "raises on a :consumer_opts key the producer sets itself" do
      assert_raise ArgumentError, ~r/:consumer_count cannot be set here.+concurrency: N/, fn ->
        Producer.init(topics: ["t"], subscription: "s", consumer_opts: [consumer_count: 4])
      end

      assert_raise ArgumentError, ~r/:flow_initial cannot be set here/, fn ->
        Producer.init(topics: ["t"], subscription: "s", consumer_opts: [flow_initial: 10])
      end
    end

    test "validates flow options before starting any consumer" do
      start_supervised!(%{
        id: :fake_client,
        start: {Agent, :start_link, [fn -> :ok end, [name: :fake_client]]}
      })

      assert_raise ArgumentError, ~r/flow_threshold \(10\) must be less than flow_initial \(10\)/, fn ->
        Producer.init(topics: ["t"], subscription: "s", client: :fake_client, flow_initial: 10, flow_threshold: 10)
      end
    end
  end

  describe "prepare_for_start/2" do
    defp broadway_opts(producer_opts) do
      [
        name: __MODULE__.Pipeline,
        producer: [module: {Producer, producer_opts}, concurrency: 1],
        processors: [default: [concurrency: 1]]
      ]
    end

    test "raises before any stage starts, so Broadway.start_link/2 reports the error" do
      assert_raise ArgumentError, ~r/required :subscription option not found/, fn ->
        Producer.prepare_for_start(__MODULE__, broadway_opts(topics: ["t"]))
      end
    end

    test "raises on a reserved :consumer_opts key" do
      opts = broadway_opts(topics: ["t"], subscription: "s", consumer_opts: [flow_initial: 10])

      assert_raise ArgumentError, ~r/:flow_initial cannot be set here/, fn ->
        Producer.prepare_for_start(__MODULE__, opts)
      end
    end

    test "raises on an invalid nested :consumer_opts value" do
      opts = broadway_opts(topics: ["t"], subscription: "s", consumer_opts: [subscription_type: :Shared])

      assert_raise ArgumentError, ~r/invalid value for :subscription_type option/, fn ->
        Producer.prepare_for_start(__MODULE__, opts)
      end
    end

    test "raises when the flow threshold is not less than the initial window" do
      opts = broadway_opts(topics: ["t"], subscription: "s", flow_initial: 10, flow_threshold: 10)

      assert_raise ArgumentError, ~r/flow_threshold \(10\) must be less than flow_initial \(10\)/, fn ->
        Producer.prepare_for_start(__MODULE__, opts)
      end
    end

    test "starts no children and returns the topology options unchanged" do
      opts = broadway_opts(topics: ["t"], subscription: "s")

      assert {[], ^opts} = Producer.prepare_for_start(__MODULE__, opts)
    end

    test "does not require the client to be running, which is the stage's own precondition" do
      opts = broadway_opts(topics: ["t"], subscription: "s", client: :no_such_client)

      assert {[], ^opts} = Producer.prepare_for_start(__MODULE__, opts)
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

      buffer =
        :queue.from_list([
          {:message, @message, dead, @context},
          {:message, @message, alive, @context},
          {:permits, dead, 2}
        ])

      state = %{state() | buffer: buffer, demand: 0, consumers: %{dead => {"topic", 10}, alive => {"other", 10}}}

      assert {:noreply, [], new_state} =
               Producer.handle_info({:DOWN, make_ref(), :process, dead, :killed}, state)

      assert :queue.to_list(new_state.buffer) == [{:message, @message, alive, @context}]
      refute Map.has_key?(new_state.consumers, dead)
    end
  end

  describe "dispatch" do
    test "wraps a message and leaves it for the acknowledger" do
      assert {:noreply, [%Message{data: "ok", metadata: metadata}], new_state} =
               Producer.handle_info({:pulsar_message, @message, self(), @context}, state())

      assert metadata.message_id_string == "1:2:-1"
      assert metadata.topic == "topic"
      assert :queue.is_empty(new_state.buffer)
      # Nothing is charged until the flow policy reports what the delivery cost.
      pid = self()
      assert %{^pid => {"topic", 10}} = new_state.consumers
    end

    test "dispatches up to demand and buffers the rest" do
      buffer =
        1..3
        |> Enum.map(fn _ -> {:message, @message, self(), @context} end)
        |> :queue.from_list()

      state = %{state() | buffer: buffer, demand: 0}

      assert {:noreply, dispatched, new_state} = Producer.handle_demand(2, state)

      assert length(dispatched) == 2
      assert :queue.len(new_state.buffer) == 1
      assert new_state.demand == 0
    end
  end

  describe ":permits_consumed" do
    test "charges the window only once Broadway has taken the delivery's messages" do
      # One delivery: two messages, then the marker saying what the broker charged for it.
      buffer =
        :queue.from_list([
          {:message, @message, self(), @context},
          {:message, @message, self(), @context},
          {:permits, self(), 2}
        ])

      pid = self()

      assert {:noreply, [_first], held} = Producer.handle_demand(1, %{state() | buffer: buffer, demand: 0})
      assert %{^pid => {"topic", 10}} = held.consumers
      assert [{:message, _, _, _}, {:permits, _, 2}] = :queue.to_list(held.buffer)

      assert {:noreply, [_second], charged} = Producer.handle_demand(1, held)
      assert %{^pid => {"topic", 8}} = charged.consumers
      assert :queue.is_empty(charged.buffer)
    end

    test "charges a delivery no callback saw without waiting for demand" do
      state = %{state() | demand: 0}

      assert {:noreply, [], new_state} = Producer.handle_info({:permits_consumed, self(), 2}, state)

      pid = self()
      assert %{^pid => {"topic", 8}} = new_state.consumers
      assert :queue.is_empty(new_state.buffer)
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
    test "stops when a consumer root exits normally" do
      monitor_ref = make_ref()
      state = %{state() | consumer_roots: %{self() => {"topic", monitor_ref}}}

      assert {:stop, {:shutdown, {:consumer_gone, "topic"}}, _state} =
               Producer.handle_info({:DOWN, monitor_ref, :process, self(), :normal}, state)
    end

    test "the health check leaves a running consumer alone" do
      %{root: root, state: state} = healthy_consumer()

      assert {:noreply, [], _state} = Producer.handle_info(:check_consumers, state)
      assert Process.alive?(root)
    end

    test "the health check stops when a consumer has a group the client gave up on" do
      %{root: root, state: state} = healthy_consumer()

      :ok = Supervisor.stop(group(root))

      assert {:stop, {:shutdown, {:consumer_stopped, "topic"}}, _state} =
               Producer.handle_info(:check_consumers, state)
    end

    test "the health check stops when a group is up but its last worker was not replaced" do
      %{root: root, state: state} = healthy_consumer()

      :ok = Agent.stop(worker(root))

      assert Process.alive?(group(root))

      assert {:stop, {:shutdown, {:consumer_stopped, "topic"}}, _state} =
               Producer.handle_info(:check_consumers, state)
    end

    test "the health check leaves a group alone while one of its workers is still up" do
      %{root: root, state: state} = healthy_consumer(worker_count: 2)

      [{_id, stopped, _type, _modules} | _kept] = Supervisor.which_children(group(root))
      :ok = Agent.stop(stopped)

      assert {:noreply, [], _state} = Producer.handle_info(:check_consumers, state)
    end
  end

  describe "unexpected messages" do
    test "are ignored rather than taking the stage and its consumer roots down" do
      assert {:noreply, [], new_state} = Producer.handle_info(:unexpected, state())

      assert new_state == state()
    end

    test "do not consume demand or disturb the buffer" do
      buffered = %{state() | buffer: :queue.from_list([{:permits, self(), 1}]), demand: 7}

      assert {:noreply, [], new_state} = Producer.handle_info({:some, :tuple}, buffered)

      assert new_state.demand == 7
      assert :queue.to_list(new_state.buffer) == [{:permits, self(), 1}]
    end
  end

  defp healthy_consumer(opts \\ []) do
    root = start_supervised!(consumer_root_spec(Keyword.get(opts, :worker_count, 1)))
    monitor_ref = Process.monitor(root)

    %{root: root, state: %{state() | consumer_roots: %{root => {"topic", monitor_ref}}}}
  end

  # Mirrors the shape the stage inspects: a root of per-topic groups, each of them a
  # supervisor of workers.
  defp consumer_root_spec(worker_count) do
    workers =
      for index <- 1..worker_count do
        %{id: {:worker, index}, start: {Agent, :start_link, [fn -> :ok end]}, restart: :transient}
      end

    group = %{
      id: {:topic, :non_partitioned},
      start: {Supervisor, :start_link, [workers, [strategy: :one_for_one]]},
      type: :supervisor,
      restart: :transient
    }

    %{
      id: :root,
      start: {Supervisor, :start_link, [[group], [strategy: :one_for_one]]},
      type: :supervisor
    }
  end

  defp group(root) do
    [{_id, group, :supervisor, _modules}] = Supervisor.which_children(root)
    group
  end

  defp worker(root) do
    [{_id, worker, _type, _modules}] = Supervisor.which_children(group(root))
    worker
  end

  defp state do
    %{
      consumers: %{self() => {"topic", 10}},
      consumer_roots: %{},
      demand: 10,
      buffer: :queue.new(),
      flow_initial: 10,
      flow_threshold: 2,
      flow_refill: 5
    }
  end
end
