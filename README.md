# Broadway Producer for Apache Pulsar

[![CI](https://github.com/efcasado/off_broadway_pulsar/actions/workflows/ci.yml/badge.svg)](https://github.com/efcasado/off_broadway_pulsar/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/efcasado/off_broadway_pulsar/badge.svg?branch=main)](https://coveralls.io/github/efcasado/off_broadway_pulsar?branch=main)
[![Package Version](https://img.shields.io/hexpm/v/off_broadway_pulsar.svg)](https://hex.pm/packages/off_broadway_pulsar)
[![hexdocs.pm](https://img.shields.io/badge/hex-docs-purple.svg)](https://hexdocs.pm/off_broadway_pulsar/)

A [Broadway](https://github.com/dashbitco/broadway) producer for [Apache Pulsar](https://pulsar.apache.org/), built on top of [pulsar-elixir](https://github.com/efcasado/pulsar-elixir/).

## Installation

Add `:off_broadway_pulsar` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_pulsar, "~> 1.5.1"} <!-- x-release-please-version -->
  ]
end
```

## Quick Start

Supervise a `Pulsar.Client` alongside your pipeline — the producer attaches its consumers
to it, so the connection is shared by every producer stage and outlives any one of them:

```elixir
children = [
  {Pulsar.Client, host: "pulsar://localhost:6650"},
  MyApp.PulsarPipeline
]
```

The consumers themselves belong to the producer stage that started them. They are linked to
it, so they go down with the pipeline rather than outliving it holding the subscription, and
the stage goes down with them rather than running on with nothing to consume from.

Then, assuming Pulsar is running on `localhost:6650`:

```elixir
defmodule MyApp.PulsarPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topics: ["persistent://public/default/my-topic"],
          subscription: "my-subscription"
        },
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 10]
      ],
      batchers: [
        default: [
          batch_size: 100,
          batch_timeout: 1000
        ]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    IO.inspect(message.data, label: "Received")
    message
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    IO.inspect(length(messages), label: "Batch size")
    messages
  end
end
```

The client defaults to `:default`. Name it to run more than one, or to consume from more
than one cluster, and select it with `:client`:

```elixir
children = [
  {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
  MyApp.AnalyticsPipeline
]

producer: [
  module: {OffBroadway.Pulsar.Producer,
    client: :analytics,
    topic: "persistent://public/default/my-topic",
    subscription: "my-subscription"
  },
  concurrency: 1
]
```

## Message metadata

Each `Broadway.Message` carries its Pulsar origin and message fields in `:metadata`:

```elixir
def handle_message(_processor, message, _context) do
  %{
    topic: topic,          # resolved topic; the concrete partition if partitioned
    base_topic: base,      # the configured topic
    partition: partition,  # partition index, or nil
    key: key,
    properties: properties
  } = message.metadata

  message
end
```

See the [producer documentation](https://hexdocs.pm/off_broadway_pulsar/OffBroadway.Pulsar.Producer.html#module-message-metadata)
for the full list.

## Failover active state

For `:failover` subscriptions, `off_broadway_pulsar` reports when an underlying
Pulsar consumer becomes active or passive through an optional callback. Configure
the callback as a `{module, function, extra_args}` tuple:

```elixir
producer: [
  module: {OffBroadway.Pulsar.Producer,
    topic: "persistent://public/default/my-topic",
    subscription: "my-subscription",
    consumer_opts: [subscription_type: :failover],
    active_state_callback: {MyApp.FailoverObserver, :handle_active_state, []}
  },
  concurrency: 1
]
```

The callback receives a metadata map (the active state, the topic or partition,
the subscription, and the consumer pid) followed by any configured extra
arguments. Reports are best-effort observations — they may repeat, and they are
not a distributed lock or fencing mechanism.

See the [producer documentation](https://hexdocs.pm/off_broadway_pulsar/OffBroadway.Pulsar.Producer.html#start_link/1)
for the complete callback contract.
