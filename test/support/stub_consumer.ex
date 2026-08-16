defmodule OffBroadwayPulsar.Test.Support.StubConsumer do
  @moduledoc false
  # Observes flow refills without a broker. Pulsar.Consumer.send_flow/3 reads
  # :proc_lib.initial_call/1, so a plain GenServer reads as a worker and is called directly.
  use GenServer

  def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

  @impl true
  def init(test_pid), do: {:ok, test_pid}

  @impl true
  def handle_call({:send_flow, permits}, _from, test_pid) do
    send(test_pid, {:send_flow, permits})
    {:reply, :ok, test_pid}
  end
end
