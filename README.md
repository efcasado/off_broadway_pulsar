# OffBroadwayPulsar

A [Broadway](https://github.com/dashbitco/broadway) producer for [Apache Pulsar](https://pulsar.apache.org/).

This library provides a Broadway producer that integrates with Apache Pulsar, allowing you to build data ingestion and processing pipelines with Broadway's features like concurrent processing, batching, automatic acknowledgements, and graceful shutdown.

## Features

- **Broadway Integration**: Leverage Broadway's robust pipeline features (batching, rate limiting, graceful shutdown)
- **Manual Acknowledgement**: Full control over message acknowledgement with Broadway's built-in acknowledger
- **Backpressure**: Broadway's demand mechanism controls Pulsar message flow
- **Dead Letter Queue**: Automatic DLQ support through the underlying Pulsar client
- **Redelivery**: Configurable message redelivery on failure

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `off_broadway_pulsar` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_pulsar, "~> 0.1.0"},
    {:broadway, "~> 1.0"}
  ]
end
```

## Usage

Define a Broadway pipeline using the Pulsar producer:

```elixir
defmodule MyApp.PulsarPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          pulsar: [
            host: "pulsar://localhost:6650"
          ],
          topic: "persistent://public/default/my-topic",
          subscription: "my-subscription"
        },
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: 10
        ]
      ],
      batchers: [
        default: [
          batch_size: 100,
          batch_timeout: 1000,
          concurrency: 5
        ]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    # Process your message here
    IO.inspect(message.data, label: "Received")
    message
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    # Process batch of messages
    IO.inspect(length(messages), label: "Batch size")
    messages
  end
end
```

## Configuration

### Producer Options

- `:pulsar` - Pulsar client configuration (required)
  - `:host` - Pulsar broker URL (e.g., `"pulsar://localhost:6650"`)
  - Other Pulsar client options
- `:topic` - Pulsar topic to consume from (required)
- `:subscription` - Subscription name (required)

The underlying Pulsar consumer can be configured through the `pulsar-elixir` library options:

## Documentation

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/off_broadway_pulsar>.

## License

Copyright (c) 2025

This project is licensed under the MIT License.

