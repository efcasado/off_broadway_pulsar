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
          host: "pulsar://localhost:6650",
          topic: "persistent://public/default/my-topic",
          subscription: "my-subscription",
          consumer_opts: [
            subscription_type: :Shared
          ]
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

- `:host` - Broker URL (e.g., `"pulsar://localhost:6650"`) (required)
- `:topic` - Pulsar topic to consume from (required)
- `:subscription` - Subscription name (required)
- `:conn_opts` - Connection options (optional):
  - `:socket_opts` - Socket options (e.g., `[verify: :verify_none]`)
  - `:auth` - Authentication configuration:
    - `:type` - Auth module (e.g., `Pulsar.Auth.OAuth2`)
    - `:opts` - Auth-specific options
  - `:conn_timeout` - Connection timeout in milliseconds
  - `:startup_jitter_ms` - Random startup delay to avoid thundering herd
  - `:start_delay_ms` - Startup delay in milliseconds
- `:consumer_opts` - Consumer-specific options (optional):
  - `:subscription_type` - Subscription type (`:Exclusive`, `:Shared`, `:Key_Shared`, default: `:Shared`)
  - `:initial_position` - Initial position (`:latest` or `:earliest`, default: `:latest`)
  - `:durable` - Whether subscription is durable (default: `true`)
  - `:force_create_topic` - Force topic creation (default: `true`)
  - `:start_message_id` - Start from specific message ID
  - `:start_timestamp` - Start from timestamp
  - `:redelivery_interval` - Redelivery interval in milliseconds for NACKed messages
  - `:dead_letter_policy` - Dead letter queue configuration:
    - `:max_redelivery` - Maximum redeliveries before sending to DLQ
    - `:topic` - Dead letter topic (optional, defaults to `<topic>-<subscription>-DLQ`)
  - `:consumer_count` - Number of consumer processes (default: 1)

**Note:** Flow control options (`:flow_initial`, `:flow_threshold`, `:flow_refill`) are not supported because Broadway controls message flow through its demand mechanism.

### Example with Authentication

```elixir
producer: [
  module: {OffBroadway.Pulsar.Producer,
    host: "pulsar+ssl://my-cluster.example.com:6651",
    topic: "persistent://my-tenant/my-namespace/my-topic",
    subscription: "my-subscription",
    conn_opts: [
      socket_opts: [verify: :verify_none],
      auth: [
        type: Pulsar.Auth.OAuth2,
        opts: [
          client_id: "my-client-id",
          client_secret: "my-client-secret",
          site: "https://auth.example.com",
          audience: "urn:pulsar:my-cluster"
        ]
      ]
    ],
    consumer_opts: [
      subscription_type: :Shared,
      initial_position: :earliest,
      dead_letter_policy: [
        max_redelivery: 3
      ]
    ]
  },
  concurrency: 1
]
```

## Documentation

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/off_broadway_pulsar>.

## License

Copyright (c) 2025

This project is licensed under the MIT License.

