# Upgrading to 2.0

2.0 moves to [pulsar-elixir](https://github.com/efcasado/pulsar-elixir/) 3.x and reworks how
the producer is configured and how it owns its consumers. The configuration changes are
rejected at boot, so a pipeline that starts is configured correctly. The metadata change is
not: reading a key that moved raises on the first message instead.

Bump the dependency:

```elixir
{:off_broadway_pulsar, "~> 2.0"}
```

## At a glance

| 1.x | 2.0 | Where |
| --- | --- | --- |
| `:host` + `:conn_opts` on the producer | Supervise `Pulsar.Client` yourself | [Connections](#connections) |
| `{Pulsar, host: ...}` in your tree | `{Pulsar.Client, host: ...}` | [Connections](#connections) |
| `topic: "my-topic"` | `topics: ["my-topic"]` | [Topics](#topics) |
| `subscription_type: :Shared` | `subscription_type: :shared` | [Subscription types](#subscription-types) |
| Unknown `:consumer_opts` keys ignored | Unknown and reserved keys raise | [Consumer options](#consumer-options) |
| `metadata.command`, `.metadata`, … | Normalized fields, raw under `:raw` | [Message metadata](#message-metadata) |

## Connections

The producer no longer starts or owns a connection. `:host` and `:conn_opts` are gone; the
client is yours to supervise, so the connection outlives any one producer stage and is shared
by all of them.

If you passed `:host` to the producer:

```elixir
# 1.x
producer: [
  module: {OffBroadway.Pulsar.Producer,
    host: "pulsar://localhost:6650",
    conn_opts: [socket_opts: [verify: :verify_none]],
    topic: "my-topic",
    subscription: "my-subscription"
  }
]

# 2.0
children = [
  {Pulsar.Client, host: "pulsar://localhost:6650", socket_opts: [verify: :verify_none]},
  MyApp.PulsarPipeline
]

producer: [
  module: {OffBroadway.Pulsar.Producer,
    topics: ["my-topic"],
    subscription: "my-subscription"
  }
]
```

`:conn_opts` keys move up to `Pulsar.Client` itself — there is no nested option group any more.

If you were already running a global connection, the module name changed:

```elixir
# 1.x
children = [{Pulsar, host: "pulsar://localhost:6650"}, MyApp.PulsarPipeline]

# 2.0
children = [{Pulsar.Client, host: "pulsar://localhost:6650"}, MyApp.PulsarPipeline]
```

`Pulsar.Client` registers as `:default` when unnamed, which is also the producer's default
`:client`, so an unnamed client needs no `:client` option. Name it to run more than one:

```elixir
children = [
  {Pulsar.Client, name: :analytics, host: "pulsar://analytics:6650"},
  MyApp.AnalyticsPipeline
]

producer: [
  module: {OffBroadway.Pulsar.Producer,
    client: :analytics,
    topics: ["my-topic"],
    subscription: "my-subscription"
  }
]
```

A producer whose client is not running raises at boot with the supervision snippet it wants,
rather than failing to consume.

## Topics

`:topic` is gone. Use `:topics`, always a non-empty list:

```elixir
# 1.x
topic: "my-topic"

# 2.0
topics: ["my-topic"]
```

An empty list is rejected — it would leave a stage running with nothing to consume.

## Subscription types

pulsar-elixir 3.x renamed the subscription type atoms to lowercase. This is the change most
likely to bite, because 1.x silently discarded a `:consumer_opts` key it did not recognise but
happily accepted a wrongly-cased value:

| 1.x | 2.0 |
| --- | --- |
| `:Exclusive` | `:exclusive` |
| `:Shared` | `:shared` |
| `:Failover` | `:failover` |
| `:Key_Shared` | `:key_shared` |

```elixir
# 1.x
consumer_opts: [subscription_type: :Failover]

# 2.0
consumer_opts: [subscription_type: :failover]
```

`:shared` remains the default. Other option values — `:initial_position`, `:start_message_id`,
`:dead_letter_policy` and the rest — keep the shapes they had.

## Consumer options

In 1.x, `:consumer_opts` was filtered through an allowlist with `Keyword.take/2`: anything the
producer did not recognise was dropped without a word, so `subsciption_type: :Failover` gave
you a Shared subscription in production. In 2.0 the keys are validated by `Pulsar.Consumer`,
which rejects what it does not know.

All fifteen consumer options 1.x supported still exist under the same names, so a correctly
spelled 1.x configuration carries over unchanged. A misspelled one now raises — which is the
point.

Two things to know:

**Reserved keys raise.** The producer sets some options itself, and setting them in
`:consumer_opts` would either be discarded or break its flow accounting. Each is rejected with
the option that does work instead:

`:client`, `:topic`, `:subscription_name`, `:callback_module`, `:init_args`, `:name`,
`:consumer_count`, `:flow_policy`, `:flow_initial`, `:flow_threshold`, `:flow_refill`

Use `producer: [concurrency: N]` for the consumer count, and the producer's own `:flow_initial`
/ `:flow_threshold` / `:flow_refill` for flow control.

**Setting `:consumer_opts` replaces the default rather than merging into it.** The default is
`[subscription_type: :shared]`, which is also `Pulsar.Consumer`'s own default, so in practice
nothing is lost — but spell out every option you want in a single list rather than expecting a
merge.

## Message metadata

1.x exposed the wire protocol structs directly, which meant knowing that a batched message
carries its key in `single_metadata` while a non-batched one carries it in `metadata`. 2.0
gives you normalized fields that answer the same way regardless of how the message was
delivered, and keeps the protocol structs under `:raw`.

```elixir
# 1.x
%{message_id: id, command: cmd, metadata: md, single_metadata: smd, broker_metadata: bmd} =
  message.metadata

# 2.0
%{command: cmd, metadata: md, single_metadata: smd, broker_metadata: bmd} = message.metadata.raw
```

`:message_id` keeps its name and its opaque role. The four protocol keys move under `:raw`.
Everything else is new:

`:message_id_string`, `:topic`, `:base_topic`, `:partition`, `:subscription`, `:key`,
`:ordering_key`, `:properties`, `:producer_name`, `:publish_time`, `:event_time`,
`:redelivery_count`

Prefer these over `:raw`, whose shape follows the wire protocol and is explicitly unstable.
Reading the key off a batched message, for instance, is now just `message.metadata.key`.

```elixir
def handle_message(_processor, message, _context) do
  %{topic: topic, partition: partition, key: key, properties: properties} = message.metadata
  message
end
```

## Behaviour changes that need no code change

These do not require edits, but they change what you will observe.

**Consumer ownership.** Each producer stage now starts and links its own consumer roots
instead of registering them under the client's `DynamicSupervisor`. A consumer that dies takes
its stage down and Broadway recreates both, where 1.x could leave a stage running with nothing
to consume. One consequence: `Pulsar.Client.consumers/1` no longer lists a pipeline's
consumers. Stop the Broadway pipeline to stop them.

**Acknowledgement failures no longer crash the pipeline.** 1.x asserted `:ok = Pulsar.ack(...)`,
so any ack failure raised inside Broadway's acknowledger and aborted the rest of that batch's
acknowledgements. 2.0 logs and continues.

**Incomplete chunked and invalid messages are acknowledged and dropped.** In 1.x an incomplete
chunked message was filtered out of the producer buffer but never acknowledged, so it held the
subscription's cursor. Both cases are now logged, acked and dropped before reaching the
pipeline.

**Consumers subscribe immediately by default.** pulsar-elixir 3.x defaults `:startup_delay_ms`
and `:startup_jitter_ms` to `0`, where 2.x defaulted both to `1000`. If you relied on that
stagger across a large fleet of restarting consumers, set them explicitly in `:consumer_opts`.

## Worth checking while you are here

Neither of these changed in 2.0, but both are easy to get wrong and easy to fix during an
upgrade.

**`Broadway.Message.failed/2` does nothing without `:redelivery_interval`.** A negative
acknowledgement is only redelivered when the consumer has a redelivery interval configured;
without one the message is not retried, and if it arrived in a batch its entry stays
unacknowledged until something acks it or the consumer restarts. If your pipeline marks
messages as failed and expects them back, set it:

```elixir
consumer_opts: [subscription_type: :shared, redelivery_interval: 60_000]
```

**Consider `:batch_index_ack_enabled` if your topics are batched.** Pulsar producers batch by
default, and an acknowledgement names the entry it lands in, not the message. Broadway
routinely completes messages from one entry out of order and in different batches, so one
failed message causes its whole entry to be redelivered — and its already-successful siblings
to be processed a second time. Enabling batch index acknowledgement narrows redelivery to just
the messages that actually failed:

```elixir
consumer_opts: [subscription_type: :shared, batch_index_ack_enabled: true]
```

It requires `acknowledgmentAtBatchIndexLevelEnabled=true` on the broker, which is the default
on Pulsar 4.2 — `broker.conf` sets it explicitly and a standalone broker inherits it, so a
cluster that has not overridden it already honours the setting. Check yours with
`bin/pulsar-admin brokers get-all-dynamic-config`, since nothing in the protocol reports it:
against a broker with it disabled, an ack acknowledges the whole entry and loses the messages
batched alongside it. It also costs one acknowledgement command per message rather than one
per entry.

Nacked messages come back only when `:redelivery_interval` is also set, so the two options go
together if you want a failed message retried rather than merely narrowed.

## Complete example

```elixir
# 1.x
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
          consumer_opts: [subscription_type: :Shared, initial_position: :earliest]
        },
        concurrency: 1
      ],
      processors: [default: [concurrency: 10]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    key = message.metadata.single_metadata && message.metadata.single_metadata.partition_key
    IO.inspect({key, message.data})
    message
  end
end
```

```elixir
# 2.0
children = [
  {Pulsar.Client, host: "pulsar://localhost:6650"},
  MyApp.PulsarPipeline
]

defmodule MyApp.PulsarPipeline do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadway.Pulsar.Producer,
          topics: ["persistent://public/default/my-topic"],
          subscription: "my-subscription",
          consumer_opts: [subscription_type: :shared, initial_position: :earliest]
        },
        concurrency: 1
      ],
      processors: [default: [concurrency: 10]]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    IO.inspect({message.metadata.key, message.data})
    message
  end
end
```

See the
[producer documentation](https://hexdocs.pm/off_broadway_pulsar/OffBroadway.Pulsar.Producer.html#start_link/1)
for every option with its type and default.
