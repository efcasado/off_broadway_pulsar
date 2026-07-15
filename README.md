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
    {:off_broadway_pulsar, "~> 1.4.0"} <!-- x-release-please-version -->
  ]
end
```

## Quick Start

Assuming you have Pulsar running on `localhost:6650`, you can create a Broadway pipeline like this:

```elixir
defmodule MyApp.PulsarPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          host: "pulsar://localhost:6650",
          topics: [persistent://public/default/my-topic"],
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

If you're running Pulsar globally in your application supervision tree, omit the `:host` option and optionally specify `:client`:

```elixir
producer: [
  module: {OffBroadway.Pulsar.Producer,
    topic: "persistent://public/default/my-topic",
    subscription: "my-subscription",
    client: :default  # Optional, defaults to :default
  },
  concurrency: 1
]
```

## Failover active state

For `:Failover` subscriptions, `off_broadway_pulsar` reports when an underlying
Pulsar consumer becomes active or passive. Configure an optional listener using
a PID, registered name, or another `GenServer.server()` destination:

```elixir
producer: [
  module: {OffBroadway.Pulsar.Producer,
    topic: "persistent://public/default/my-topic",
    subscription: "my-subscription",
    consumer_opts: [subscription_type: :Failover],
    active_state_listener: MyApp.PulsarOwnershipListener
  },
  concurrency: 1
]
```

The listener receives an ordinary process message and handles the work outside
the Pulsar consumer and Broadway producer:

```elixir
def handle_info(
      {:off_broadway_pulsar, :consumer_active_state_changed, metadata},
      state
    ) do
  # metadata.active_state is :active or :passive
  {:noreply, state}
end
```

Every transition also emits
`[:off_broadway_pulsar, :consumer, :active_state_changed]` through Telemetry.
Its measurements contain `:system_time`, and its metadata contains
`:active_state`, `:topic`, `:subscription`, `:consumer_pid`, and `:producer_pid`.

Active state belongs to an individual topic or partition. It is not a general
distributed lock or a guarantee that messages already delivered to Broadway
have finished processing. Partitioned topics, multiple topics, and producer
concurrency greater than one can give a pipeline multiple simultaneous consumer
states.
