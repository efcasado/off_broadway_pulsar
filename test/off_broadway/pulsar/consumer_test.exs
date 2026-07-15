defmodule OffBroadway.Pulsar.ConsumerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Consumer

  describe "terminate/2" do
    test "is defined and returns :ok" do
      assert Consumer.terminate(:normal, %{}) == :ok
    end

    test "is defined for any reason and state" do
      assert Consumer.terminate(:shutdown, %{broadway_producer: self(), topic: "t"}) == :ok
      assert Consumer.terminate({:shutdown, :reason}, nil) == :ok
    end
  end

  describe "handle_call/3" do
    test "returns not_implemented error by default" do
      assert Consumer.handle_call(:anything, {self(), :tag}, %{}) ==
               {:reply, {:error, :not_implemented}, %{}}
    end
  end

  describe "handle_cast/2" do
    test "returns noreply by default" do
      assert Consumer.handle_cast(:anything, %{}) == {:noreply, %{}}
    end
  end

  describe "handle_info/2" do
    test "returns noreply by default" do
      assert Consumer.handle_info(:anything, %{}) == {:noreply, %{}}
    end
  end

  describe "Failover active state" do
    test "forwards active transitions to the Broadway producer" do
      state = %{broadway_producer: self(), topic: "my-topic"}

      assert Consumer.became_active(state) == {:noreply, state}
      assert_receive {:consumer_active_state_changed, :active, consumer_pid, "my-topic"}
      assert consumer_pid == self()
    end

    test "forwards passive transitions to the Broadway producer" do
      state = %{broadway_producer: self(), topic: "my-topic"}

      assert Consumer.became_passive(state) == {:noreply, state}
      assert_receive {:consumer_active_state_changed, :passive, consumer_pid, "my-topic"}
      assert consumer_pid == self()
    end
  end
end
