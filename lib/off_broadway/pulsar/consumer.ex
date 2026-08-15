defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  require Logger

  @impl true
  def init([broadway_producer, active_state_callback], context) do
    # The worker PID identifies its permit window; context supplies message metadata.
    send(broadway_producer, {:consumer_ready, self(), context})

    {:ok,
     %{
       broadway_producer: broadway_producer,
       context: context,
       active_state_callback: active_state_callback
     }}
  end

  @impl true
  def handle_message(%Pulsar.Message{} = message, state) do
    if Pulsar.Message.complete?(message) do
      send(state.broadway_producer, {:pulsar_message, message, self(), state.context})
      {:noreply, state}
    else
      # :ok hands the ack to the worker; left unacked it would hold the subscription's cursor.
      Logger.warning("Discarding incomplete chunked message")

      {:ok, state}
    end
  end

  # Flow policy, configured as {__MODULE__, :report_permits, [broadway_producer]}. Reports
  # rather than grants: the producer refills as Broadway consumes, which is what stops the
  # broker running further ahead than the pipeline has asked for.
  def report_permits(%{consumed: 0}, _broadway_producer), do: :ok

  def report_permits(%{consumed: consumed}, broadway_producer) do
    send(broadway_producer, {:permits_consumed, self(), consumed})
    :ok
  end

  @impl true
  def became_active(state) do
    invoke_active_state_callback(state, :active)

    {:noreply, state}
  end

  @impl true
  def became_passive(state) do
    invoke_active_state_callback(state, :passive)

    {:noreply, state}
  end

  defp invoke_active_state_callback(%{active_state_callback: nil}, _active_state), do: :ok

  defp invoke_active_state_callback(state, active_state) do
    {module, function, extra_args} = state.active_state_callback

    metadata = %{
      active_state: active_state,
      topic: state.context.topic,
      subscription: state.context.subscription_name,
      consumer_pid: self()
    }

    apply(module, function, [metadata | extra_args])
  end
end
