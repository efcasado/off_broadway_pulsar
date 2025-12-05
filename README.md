[![CI](https://github.com/efcasado/off_broadway_pulsar/actions/workflows/ci.yml/badge.svg)](https://github.com/efcasado/off_broadway_pulsar/actions/workflows/ci.yml)
[![Package Version](https://img.shields.io/hexpm/v/off_broadway_pulsar.svg)](https://hex.pm/packages/off_broadway_pulsar)
[![hexdocs.pm](https://img.shields.io/badge/hex-docs-purple.svg)](https://hexdocs.pm/off_broadway_pulsar/)

# Broadway Producer for Pulsar

A [Broadway](https://github.com/dashbitco/broadway) producer for [Apache Pulsar](https://pulsar.apache.org/).

This library provides a Broadway producer that integrates with Apache Pulsar, allowing you to build data ingestion and processing pipelines with Broadway's features like concurrent processing, batching, automatic acknowledgements, and graceful shutdown.

Underneath, this library uses [pulsar-elixir](https://github.com/efcasado/pulsar-elixir/) to interact with Pulsar.

## Features

- **Broadway Integration**: Leverage Broadway's robust pipeline features (batching, rate limiting, graceful shutdown)
- **Manual Acknowledgement**: Full control over message acknowledgement with Broadway's built-in acknowledger
- **Flow Control**: Permit window-based flow control for efficient message batching
- **Dead Letter Queue**: Automatic DLQ support through the underlying Pulsar client
- **Redelivery**: Configurable message redelivery on failure

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `off_broadway_pulsar` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_pulsar, "~> 1.0.1"}
  ]
end
```

## Usage

There are two ways to use OffBroadway.Pulsar:

### Pattern 1: Producer-Managed Connection

The producer starts its own Pulsar connection. This is simpler for getting started or when each producer needs different connection settings.

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

### Pattern 2: Application-Managed Connection

Pulsar is started globally in your application's supervision tree. This is better for production when you have multiple producers/consumers sharing the same cluster.

```elixir
# lib/my_app/application.ex
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      # Start Pulsar globally
      {Pulsar, host: "pulsar://localhost:6650"},
      # Start your Broadway pipelines
      MyApp.PulsarPipeline
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

# lib/my_app/pulsar_pipeline.ex
defmodule MyApp.PulsarPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          # No host needed - use global connection
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
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    IO.inspect(message.data, label: "Received")
    message
  end
end
```

## Configuration

### Producer Options

- `:host` - Broker URL (e.g., `"pulsar://localhost:6650"`) (optional). If provided, the producer starts its own Pulsar connection. If omitted, assumes Pulsar is already started globally.
- `:topic` - Pulsar topic to consume from (required)
- `:subscription` - Subscription name (required)
- `:conn_opts` - Connection options (optional, only used if `:host` is provided):
  - `:socket_opts` - Socket options (e.g., `[verify: :verify_none]`)
  - `:auth` - Authentication configuration:
    - `:type` - Auth module (e.g., `Pulsar.Auth.OAuth2`)
    - `:opts` - Auth-specific options
  - `:conn_timeout` - Connection timeout in milliseconds
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
  - `:startup_delay_ms` - Fixed startup delay in milliseconds before consumer initialization (default: 0)
  - `:startup_jitter_ms` - Random startup delay (0 to N ms) to avoid thundering herd on consumer restart (default: 0)
- `:flow_initial` - Initial permits requested at startup (optional, default: 100)
- `:flow_threshold` - Trigger refill when permits drop to this level (optional, default: 50)
- `:flow_refill` - Number of permits to request on each refill (optional, default: 50)

**Note:** The producer uses Pulsar's permit window flow control mechanism. Broadway processor demands are satisfied from the already-requested permit window, eliminating per-demand flow requests. When using `producer: [concurrency: N]` with N > 1, each producer maintains its own independent permit window.

### Scaling Consumers

To scale message consumption, use Broadway's `producer: [concurrency: N]` option. Each producer instance starts its own Pulsar consumer, allowing parallel consumption:

```elixir
producer: [
  module: {OffBroadway.Pulsar.Producer, ...},
  concurrency: 3  # Start 3 concurrent producers/consumers
]
```

For `:Shared` subscriptions, multiple consumers will automatically share the message load.

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

