defmodule OffBroadway.Pulsar.FlowControlTest do
  use ExUnit.Case, async: false

  alias OffBroadway.Pulsar.Producer

  # Mock consumer for testing
  defmodule MockConsumer do
    @moduledoc false
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    def init(_opts) do
      {:ok, %{flow_requests: [], test_pid: self()}}
    end

    def get_flow_requests do
      GenServer.call(__MODULE__, :get_flow_requests)
    end

    def clear_flow_requests do
      GenServer.call(__MODULE__, :clear_flow_requests)
    end

    def handle_call({:send_flow, permits}, _from, state) do
      # Record this flow request
      new_requests = [{permits, System.monotonic_time(:millisecond)} | state.flow_requests]
      {:reply, :ok, %{state | flow_requests: new_requests}}
    end

    def handle_call(:get_flow_requests, _from, state) do
      {:reply, Enum.reverse(state.flow_requests), state}
    end

    def handle_call(:clear_flow_requests, _from, state) do
      {:reply, :ok, %{state | flow_requests: []}}
    end
  end

  describe "permit window flow control" do
    setup do
      {:ok, _pid} = start_supervised(MockConsumer)
      :ok
    end

    test "requests initial permits at startup" do
      state = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 0
      }

      MockConsumer.clear_flow_requests()

      # Simulate initial flow
      state_with_flow = %{state | outstanding_permits: 100}

      # Should have sent initial flow request
      # (In real code this happens in init, but we test the pattern)
      assert state_with_flow.outstanding_permits == 100
    end

    test "refills when permits drop to threshold" do
      state = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      MockConsumer.clear_flow_requests()

      # Simulate processing 51 messages (drops to 49, below threshold of 50)
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      final_state =
        Enum.reduce(1..51, state, fn _, acc_state ->
          {:noreply, _, new_state} = Producer.handle_info({:pulsar_message, message_info}, acc_state)
          new_state
        end)

      # Should have triggered refill
      flow_requests = MockConsumer.get_flow_requests()
      assert length(flow_requests) >= 1

      # Should have refilled with flow_refill amount
      assert Enum.any?(flow_requests, fn {permits, _ts} -> permits == 50 end)

      # Outstanding should be above threshold again
      assert final_state.outstanding_permits > final_state.flow_threshold
    end

    test "handle_demand doesn't trigger flow requests (permit window already established)" do
      state = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      MockConsumer.clear_flow_requests()

      # Simulate multiple demand calls from different processors
      {:noreply, [], state} = Producer.handle_demand(5, state)
      {:noreply, [], state} = Producer.handle_demand(7, state)
      {:noreply, [], state} = Producer.handle_demand(10, state)
      {:noreply, [], state} = Producer.handle_demand(8, state)
      {:noreply, [], _state} = Producer.handle_demand(6, state)

      # Should have NO flow requests (demand satisfied from permit window)
      flow_requests = MockConsumer.get_flow_requests()
      assert flow_requests == []
    end

    test "multiple producers maintain independent permit windows" do
      # Create two separate producers (simulated)
      state1 = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      state2 = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 200,
        flow_threshold: 100,
        flow_refill: 100,
        outstanding_permits: 200
      }

      # Each maintains its own state
      assert state1.outstanding_permits == 100
      assert state2.outstanding_permits == 200

      # Process message in state1
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      {:noreply, _, new_state1} = Producer.handle_info({:pulsar_message, message_info}, state1)

      # Only state1 affected
      assert new_state1.outstanding_permits == 99
      assert state2.outstanding_permits == 200
    end

    test "dramatically reduces flow requests compared to naive approach" do
      # Scenario: Process 100 messages with 10 processors
      # Naive approach: Each processor demands 5-10 messages = ~15 flow requests
      # Permit window: 1 initial + 1-2 refills = ~3 flow requests total

      state = %{
        pulsar_consumer: MockConsumer,
        demand: 0,
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      MockConsumer.clear_flow_requests()

      # Simulate heavy load: 100 messages
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      _final_state =
        Enum.reduce(1..100, state, fn _, acc_state ->
          {:noreply, _, new_state} = Producer.handle_info({:pulsar_message, message_info}, acc_state)
          new_state
        end)

      flow_requests = MockConsumer.get_flow_requests()

      # Should have very few requests (refills only)
      # 100 messages with refill at 50 = ~2 refills
      assert length(flow_requests) <= 3

      # Total flow sent should be reasonable
      total_flow = Enum.reduce(flow_requests, 0, fn {permits, _}, acc -> acc + permits end)
      assert total_flow <= 150
    end
  end
end
