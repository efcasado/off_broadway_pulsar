defmodule OffBroadwayPulsar.Test.Support.DummyPipeline do
  @moduledoc false
  use Broadway

  def start_link(opts) do
    test_pid = Keyword.fetch!(opts, :test_pid)
    subscription = Keyword.get(opts, :subscription, "test-subscription")
    consumer_opts = Keyword.get(opts, :consumer_opts, initial_position: :earliest)
    handler = Keyword.get(opts, :handler, :default)
    name = Keyword.get(opts, :name, __MODULE__)
    producer_concurrency = Keyword.get(opts, :producer_concurrency, 1)
    processor_concurrency = Keyword.get(opts, :processor_concurrency, 1)

    # Support both :topic (single, backwards-compatible) and :topics (list).
    topic_config =
      if Keyword.has_key?(opts, :topics) do
        [topics: Keyword.fetch!(opts, :topics)]
      else
        [topic: Keyword.get(opts, :topic, "persistent://public/default/test-topic")]
      end

    producer_config =
      [subscription: subscription, consumer_opts: consumer_opts] ++
        topic_config ++
        List.wrap(if client = Keyword.get(opts, :client), do: {:client, client})

    producer_config =
      producer_config
      |> maybe_put(opts, :flow_initial)
      |> maybe_put(opts, :flow_threshold)
      |> maybe_put(opts, :flow_refill)
      |> maybe_put(opts, :active_state_callback)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {OffBroadway.Pulsar.Producer, producer_config},
        concurrency: producer_concurrency
      ],
      processors: [
        default: [concurrency: processor_concurrency]
      ],
      context: %{test_pid: test_pid, handler: handler}
    )
  end

  defp maybe_put(config, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(config, key, value)
      :error -> config
    end
  end

  @impl true
  def handle_message(processor, message, context) do
    case context.handler do
      :nack ->
        send(context.test_pid, {:message_handled, message})
        Broadway.Message.failed(message, "intentional failure")

      handler when is_function(handler, 2) ->
        handler.(message, context)

      _ ->
        send(context.test_pid, {:message_handled, message, processor})
        message
    end
  end
end
