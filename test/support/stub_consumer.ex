defmodule OffBroadwayPulsar.Test.Support.StubConsumer do
  @moduledoc false
  # Observes flow refills and acknowledgements without a broker. Pulsar.Consumer reaches a
  # worker with a GenServer.call, so a plain GenServer can answer for one.
  use GenServer

  def start_link({test_pid, reply}), do: GenServer.start_link(__MODULE__, {test_pid, reply})
  def start_link(test_pid) when is_pid(test_pid), do: start_link({test_pid, :ok})

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:send_flow, permits}, _from, {test_pid, _reply} = state) do
    send(test_pid, {:send_flow, permits})
    {:reply, :ok, state}
  end

  def handle_call({operation, message_ids}, _from, {test_pid, reply} = state) when operation in [:ack, :nack] do
    send(test_pid, {operation, message_ids})
    {:reply, reply, state}
  end
end
