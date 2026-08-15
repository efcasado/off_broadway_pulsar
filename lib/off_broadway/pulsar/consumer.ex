defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  @impl true
  def init([broadway_producer, active_state_callback], context) do
    # Asks the producer for the initial flow, on first start and on every restart. The
    # context is its per-consumer key and the origin it stamps on each Broadway message.
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
    # self() is the PID the acknowledger routes ACKs/NACKs back to.
    send(state.broadway_producer, {:pulsar_message, message, self(), state.context})

    # :noreply leaves the message unacknowledged; Broadway acks it via the acknowledger.
    {:noreply, state}
  end

  # The default implementation acks and drops, which would hide the message from the
  # producer entirely — and the broker charged a permit for it. Forwarding it keeps the
  # permit window accurate; the producer drops it before the pipeline and acks it.
  @impl true
  def handle_invalid_message(%Pulsar.Message{} = message, state) do
    handle_message(message, state)
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
