defmodule OffBroadway.Pulsar.ConsumerTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Consumer
  alias OffBroadwayPulsar.Test.Support.Utils

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
    test "invokes the MFA callback for active transitions" do
      state = active_state_callback_state()

      assert Consumer.became_active(state) == {:noreply, state}

      assert_receive {:active_state_callback,
                      %{
                        active_state: :active,
                        topic: "my-topic",
                        subscription: "my-subscription",
                        consumer_pid: consumer_pid
                      }, :configured_argument}

      assert consumer_pid == self()
    end

    test "invokes the MFA callback for passive transitions" do
      state = active_state_callback_state()

      assert Consumer.became_passive(state) == {:noreply, state}

      assert_receive {:active_state_callback,
                      %{
                        active_state: :passive,
                        topic: "my-topic",
                        subscription: "my-subscription",
                        consumer_pid: consumer_pid
                      }, :configured_argument}

      assert consumer_pid == self()
    end

    test "handles transitions when no callback is configured" do
      state = %{active_state_callback_state() | active_state_callback: nil}

      assert Consumer.became_active(state) == {:noreply, state}
      assert Consumer.became_passive(state) == {:noreply, state}
      refute_receive {:active_state_callback, _, _}
    end

    test "propagates callback exceptions" do
      state = %{
        active_state_callback_state()
        | active_state_callback: {__MODULE__, :raise_from_active_state, []}
      }

      assert_raise RuntimeError, "intentional callback failure", fn ->
        Consumer.became_active(state)
      end
    end
  end

  def raise_from_active_state(_metadata), do: raise("intentional callback failure")

  defp active_state_callback_state do
    %{
      broadway_producer: self(),
      topic: "my-topic",
      subscription: "my-subscription",
      active_state_callback: {Utils, :notify_active_state, [self(), :configured_argument]}
    }
  end
end
