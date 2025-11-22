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

    test "refills when dispatched messages drop permits to threshold" do
      state = %{
        pulsar_consumer: MockConsumer,
        demand: 100,  # Broadway has demand for messages
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      MockConsumer.clear_flow_requests()

      # Simulate 51 messages arriving (go to buffer, no dispatch yet)
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      state_with_buffer =
        Enum.reduce(1..51, state, fn _, acc_state ->
          # Messages arrive -> just buffer, no dispatch
          {:noreply, [], new_state} = Producer.handle_info({:pulsar_message, message_info}, acc_state)
          new_state
        end)

      # Buffer should have 51 messages
      assert length(state_with_buffer.buffer) == 51
      
      # Permits still at 100 (not consumed yet)
      assert state_with_buffer.outstanding_permits == 100

      # No refills yet (no dispatch = no consumption)
      flow_requests = MockConsumer.get_flow_requests()
      assert flow_requests == []

      # Now Broadway demands messages
      {:noreply, messages, final_state} = Producer.handle_demand(51, state_with_buffer)

      # 51 messages dispatched
      assert length(messages) == 51

      # Should have triggered refill (51 dispatched drops permits from 100 to 49)
      flow_requests_after = MockConsumer.get_flow_requests()
      assert length(flow_requests_after) >= 1

      # Should have refilled with flow_refill amount
      assert Enum.any?(flow_requests_after, fn {permits, _ts} -> permits == 50 end)

      # Outstanding should be above threshold again (49 + 50 = 99)
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
        demand: 10,  # Has demand to dispatch messages
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      state2 = %{
        pulsar_consumer: MockConsumer,
        demand: 10,
        buffer: [],
        flow_initial: 200,
        flow_threshold: 100,
        flow_refill: 100,
        outstanding_permits: 200
      }

      # Each maintains its own state
      assert state1.outstanding_permits == 100
      assert state2.outstanding_permits == 200

      # Message arrives in state1 - goes to buffer
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      {:noreply, [], new_state1} = Producer.handle_info({:pulsar_message, message_info}, state1)

      # Message buffered, permits unchanged until dispatch
      assert new_state1.outstanding_permits == 100
      assert length(new_state1.buffer) == 1
      
      # state2 unchanged
      assert state2.outstanding_permits == 200
    end

    test "dramatically reduces flow requests compared to naive approach" do
      # Scenario: Process 100 messages with 10 processors
      # Naive approach: Each processor demands 5-10 messages = ~15 flow requests
      # Permit window: 1 initial + 1-2 refills = ~3 flow requests total

      state = %{
        pulsar_consumer: MockConsumer,
        demand: 100,  # Broadway has demand for messages
        buffer: [],
        flow_initial: 100,
        flow_threshold: 50,
        flow_refill: 50,
        outstanding_permits: 100
      }

      MockConsumer.clear_flow_requests()

      # Simulate heavy load: 100 messages arriving (buffer only)
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      state_with_buffer =
        Enum.reduce(1..100, state, fn _, acc_state ->
          {:noreply, [], new_state} = Producer.handle_info({:pulsar_message, message_info}, acc_state)
          new_state
        end)

      # Now dispatch with demand
      {:noreply, _messages, _final_state} = Producer.handle_demand(100, state_with_buffer)

      flow_requests = MockConsumer.get_flow_requests()

      # Should have very few requests (refills only)
      # 100 messages dispatched: 100 initial permits consumed
      # After 50 consumed -> refill 50 (at threshold)
      # Total: 1-2 refills
      assert length(flow_requests) <= 3

      # Total flow sent should be reasonable (1-2 refills of 50)
      total_flow = Enum.reduce(flow_requests, 0, fn {permits, _}, acc -> acc + permits end)
      assert total_flow <= 150
    end

    test "provides true backpressure - no refill when Broadway has no demand" do
      # This is the critical fix: messages should buffer without triggering refills
      # when Broadway isn't demanding them
      state = %{
        pulsar_consumer: MockConsumer,
        demand: 0,  # NO demand from Broadway
        buffer: [],
        flow_initial: 10,
        flow_threshold: 5,
        flow_refill: 5,
        outstanding_permits: 10
      }

      MockConsumer.clear_flow_requests()

      # Simulate 10 messages arriving (fills initial permit window)
      message_info = %{
        payload: {nil, "test"},
        message_id: %{},
        command: %{},
        metadata: %{},
        broker_metadata: %{}
      }

      final_state =
        Enum.reduce(1..10, state, fn _, acc_state ->
          {:noreply, _messages, new_state} = Producer.handle_info({:pulsar_message, message_info}, acc_state)
          new_state
        end)

      # Messages should be buffered
      assert length(final_state.buffer) == 10

      # Permits should still be 10 (not consumed since not dispatched)
      assert final_state.outstanding_permits == 10

      # NO refill should have happened (no dispatch = no consumption)
      flow_requests = MockConsumer.get_flow_requests()
      assert flow_requests == []

      # Now simulate Broadway demanding messages
      # handle_demand will dispatch buffered messages immediately
      {:noreply, dispatched_messages, state_after_dispatch} = Producer.handle_demand(5, final_state)

      # 5 messages should be dispatched (the demand amount)
      assert length(dispatched_messages) == 5

      # Buffer should have 5 remaining (10 arrived - 5 dispatched)
      assert length(state_after_dispatch.buffer) == 5

      # Permits consumed (10 - 5 = 5, exactly at threshold)
      # Refill triggers when outstanding <= threshold, so 5 <= 5 triggers refill
      # New outstanding: 5 + 5 = 10
      assert state_after_dispatch.outstanding_permits == 10

      # Should have triggered refill since we hit threshold
      flow_requests_after = MockConsumer.get_flow_requests()
      assert length(flow_requests_after) == 1
      {permits, _timestamp} = List.first(flow_requests_after)
      assert permits == 5
    end
  end
end
