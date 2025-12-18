defmodule OffBroadwayPulsar.Test.Support.DummyConsumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  @impl true
  def init([test_pid]) do
    {:ok, test_pid}
  end

  @impl true
  def handle_message(%Pulsar.Message{payload: payload}, test_pid) do
    send(test_pid, {:pulsar_message, payload})
    {:ok, test_pid}
  end
end
