defmodule OffBroadwayPulsar.Test.Support.StubConsumer do
  @moduledoc false
  # Stands in for a Pulsar.Consumer.Worker so acknowledgement can be observed without
  # a broker. Pulsar.Consumer.ack/2 and nack/2 are GenServer calls to the worker pid.
  use GenServer

  def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

  @impl true
  def init(test_pid), do: {:ok, test_pid}

  @impl true
  def handle_call({op, message_ids}, _from, test_pid) when op in [:ack, :nack] do
    send(test_pid, {op, message_ids})
    {:reply, :ok, test_pid}
  end
end
