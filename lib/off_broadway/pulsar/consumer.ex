defmodule OffBroadway.Pulsar.Consumer do
  @moduledoc false
  use Pulsar.Consumer.Callback

  require Logger

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
    if Pulsar.Message.complete?(message) do
      # self() is the PID the acknowledger acks to; :noreply leaves the message
      # unacknowledged until Broadway gets there.
      send(state.broadway_producer, {:pulsar_message, message, self(), state.context})
      {:noreply, state}
    else
      drop(message, state)
    end
  end

  @impl true
  def handle_invalid_message(message, state), do: drop(message, state)

  # `:ok` hands the ack to the worker, whose own ack carries the validation error that
  # Pulsar.Consumer.ack/2 cannot. The producer hears about it only to take the permit the
  # broker charged off its window, which nothing else would.
  defp drop(message, state) do
    permits = Pulsar.Message.num_broker_messages(message)

    Logger.warning("Dropping undeliverable message: #{message.validation_error || :incomplete_chunked_message}")
    send(state.broadway_producer, {:permits_consumed, state.context, permits})

    {:ok, state}
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
