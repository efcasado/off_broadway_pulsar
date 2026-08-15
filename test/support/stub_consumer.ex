defmodule OffBroadwayPulsar.Test.Support.StubConsumer do
  @moduledoc false
  # Stands in for a Pulsar.Consumer.Worker, which is what Pulsar.Consumer.ack/2 calls,
  # so acknowledgement can be observed without a broker.
  use GenServer

  def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

  @impl true
  def init(test_pid), do: {:ok, test_pid}

  @impl true
  def handle_call({:ack, message_ids}, _from, test_pid) do
    send(test_pid, {:ack, message_ids})
    {:reply, :ok, test_pid}
  end
end
