#!/usr/bin/env elixir

# AT Protocol feed example
#
# Consumes the public AT Protocol feed over a websocket, publishes every
# event to a Pulsar topic, and processes the stream with a Broadway pipeline
# that keeps live statistics: posting activity, languages, trending hashtags
# and the most liked/reposted posts.
#
# Usage:
#   make up                       # start the local Pulsar standalone
#   elixir examples/atproto.exs   # then Ctrl-C twice to stop
#
# The stream is scoped to posts, likes and reposts, which keeps the volume
# comfortable for a local broker. Widen it by editing @url below
# (e.g. wantedCollections=app.bsky.* for every Bluesky collection, or no
# wantedCollections at all for the raw feed).

Mix.install([
  {:off_broadway_pulsar, path: Path.join(__DIR__, "..")},
  {:websockex, "~> 0.5.1"},
  {:jason, "~> 1.4"}
])

defmodule ATProto.FeedClient do
  @moduledoc false

  use WebSockex

  require Logger

  # The feed filters server-side via repeated `wantedCollections` params.
  # Account and identity events are delivered regardless of the filter.
  @host "wss://jetstream1.us-east.bsky.network/subscribe"
  @collections ~w(app.bsky.feed.post app.bsky.feed.like app.bsky.feed.repost)
  @url @host <> "?" <> Enum.map_join(@collections, "&", &"wantedCollections=#{&1}")

  @producer "atproto-producer"

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(_opts) do
    # The producer answers {:error, :not_ready} until its topic is discovered,
    # so don't open the socket before it is.
    :ok = Pulsar.Producer.await_ready(@producer, timeout: 30_000)
    Logger.info("Connected to #{@producer}, subscribing to #{@url}")

    WebSockex.start_link(@url, __MODULE__, nil)
  end

  @impl true
  def handle_frame({:text, frame}, state) do
    # Events are published exactly as received: the raw JSON is the payload,
    # and the fields the pipeline routes on travel as message properties
    # (surfaced on the other side as Broadway.Message metadata).
    {did, properties} = frame_properties(frame)

    case Pulsar.Producer.send_async(@producer, frame,
           partition_key: did,
           properties: properties
         ) do
      {:ok, _ref} -> :ok
      {:error, reason} -> Logger.warning("Failed to publish event: #{inspect(reason)}")
    end

    {:ok, state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    # send_async/3 replies land in the caller's mailbox and must be drained.
    if !match?({:ok, _}, result) do
      Logger.warning("Event not acknowledged by the broker: #{inspect(result)}")
    end

    {:ok, state}
  end

  def handle_info(_other, state), do: {:ok, state}

  @impl true
  def handle_disconnect(%{reason: reason}, state) do
    # The feed accepts a `cursor` param (a `time_us` timestamp) to resume
    # roughly where a client left off; keeping the example reconnection-naive.
    Logger.warning("Disconnected from the feed (#{inspect(reason)}), reconnecting...")
    {:reconnect, state}
  end

  defp frame_properties(frame) do
    case Jason.decode(frame) do
      {:ok, event} ->
        commit = Map.get(event, "commit", %{})

        properties =
          commit
          |> Map.take(["collection", "operation"])
          |> Map.put_new("kind", Map.get(event, "kind", "unknown"))

        {Map.get(event, "did"), properties}

      {:error, _reason} ->
        {nil, %{"kind" => "unknown"}}
    end
  end
end

defmodule ATProto.Stats do
  @moduledoc false

  # Live counters for the AT Protocol feed, fed (in batches) by the pipeline,
  # plus a periodic console report of what the stream looks like.

  use GenServer

  # Cap on how many keys each trimmed counter map keeps, to bound memory on
  # long runs. Trimming keeps the top of the table, which is all we display.
  @max_tracked_keys 1_000

  @interval_ms 10_000

  @top_languages 5
  @top_hashtags 10
  @top_posts 5

  defstruct totals: %{},
            languages: %{},
            hashtags: %{},
            liked: %{},
            reposted: %{},
            replies: 0

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def ingest_posts(summary), do: GenServer.cast(__MODULE__, {:ingest_posts, summary})
  def ingest_likes(uris), do: GenServer.cast(__MODULE__, {:ingest_likes, uris})
  def ingest_reposts(uris), do: GenServer.cast(__MODULE__, {:ingest_reposts, uris})

  @impl true
  def init(_opts) do
    {:ok, schedule_report(%__MODULE__{})}
  end

  @impl true
  def handle_cast({:ingest_posts, summary}, stats) do
    %{
      total: total,
      languages: languages,
      hashtags: hashtags,
      replies: replies
    } = summary

    {:noreply,
     %{
       stats
       | totals: merge_counts(stats.totals, %{"app.bsky.feed.post" => total}),
         languages: stats.languages |> merge_counts(languages) |> trim(),
         hashtags: stats.hashtags |> merge_counts(hashtags) |> trim(),
         replies: stats.replies + replies
     }}
  end

  def handle_cast({:ingest_likes, uris}, stats) do
    {:noreply,
     %{
       stats
       | totals: merge_counts(stats.totals, %{"app.bsky.feed.like" => length(uris)}),
         liked: stats.liked |> merge_counts(count_by(uris)) |> trim()
     }}
  end

  def handle_cast({:ingest_reposts, uris}, stats) do
    {:noreply,
     %{
       stats
       | totals: merge_counts(stats.totals, %{"app.bsky.feed.repost" => length(uris)}),
         reposted: stats.reposted |> merge_counts(count_by(uris)) |> trim()
     }}
  end

  @impl true
  def handle_info(:report, stats) do
    stats |> report() |> IO.puts()

    {:noreply, schedule_report(stats)}
  end

  defp schedule_report(stats) do
    Process.send_after(self(), :report, @interval_ms)
    stats
  end

  defp count_by(enumerable), do: Enum.frequencies(enumerable)

  defp merge_counts(counts, new), do: Map.merge(counts, new, fn _key, a, b -> a + b end)

  # Counter maps other than :totals grow unboundedly if left alone; keep the
  # top of the table. :totals stays exact — its keys (collection names) are few.
  defp trim(counts) when map_size(counts) <= @max_tracked_keys, do: counts

  defp trim(counts) do
    counts
    |> Enum.sort_by(fn {_key, count} -> count end, :desc)
    |> Enum.take(@max_tracked_keys)
    |> Map.new()
  end

  defp report(stats) do
    posts = count(stats, "app.bsky.feed.post")

    """
    [atproto] #{posts} posts (#{percent(stats.replies, posts)} replies)

      Activity  #{count(stats, "app.bsky.feed.like")} likes, #{count(stats, "app.bsky.feed.repost")} reposts
      Languages #{top(stats.languages, @top_languages)}
      Hashtags  #{top(stats.hashtags, @top_hashtags, &"##{&1}")}
      Liked     #{top(stats.liked, @top_posts, &link/1)}
      Reposted  #{top(stats.reposted, @top_posts, &link/1)}
    """
  end

  # at:// URIs are for machines; render post URIs as bsky.app URLs one can click.
  defp link("at://" <> uri) do
    case String.split(uri, "/") do
      [did, "app.bsky.feed.post", rkey] -> "https://bsky.app/profile/#{did}/post/#{rkey}"
      _other -> "at://" <> uri
    end
  end

  defp top(counts, limit, format_uri \\ & &1) do
    counts
    |> Enum.sort_by(fn {_key, count} -> count end, :desc)
    |> Enum.take(limit)
    |> Enum.map_join(", ", fn {key, count} -> "#{format_uri.(key)} (#{count})" end)
    |> case do
      "" -> "-"
      formatted -> formatted
    end
  end

  defp count(stats, collection), do: Map.get(stats.totals, collection, 0)

  defp percent(_part, 0), do: "0%"

  defp percent(part, total), do: "#{Float.round(part / total * 100, 1)}%"
end

defmodule ATProto.Pipeline do
  @moduledoc false

  use Broadway

  alias ATProto.Stats
  alias Broadway.Message

  @topic "persistent://public/default/atproto-feed"
  @subscription "atproto-stats"

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadway.Pulsar.Producer,
           topic: @topic, subscription: @subscription, consumer_opts: [initial_position: :earliest]},
        concurrency: 2
      ],
      processors: [default: [concurrency: 4]],
      batchers: [
        posts: [batch_size: 100, batch_timeout: 1_000, concurrency: 2],
        likes: [batch_size: 500, batch_timeout: 1_000, concurrency: 2],
        reposts: [batch_size: 500, batch_timeout: 1_000, concurrency: 2],
        default: [batch_size: 100, batch_timeout: 1_000, concurrency: 1]
      ]
    )
  end

  @impl true
  def handle_message(_processor, message, _context) do
    case Jason.decode(message.data) do
      {:ok, event} -> route(message, event)
      {:error, reason} -> Message.failed(message, reason)
    end
  end

  @impl true
  def handle_batch(:posts, messages, _batch_info, _context) do
    messages
    |> Enum.map(fn %Message{data: post} -> post end)
    |> summarize_posts()
    |> Stats.ingest_posts()

    messages
  end

  def handle_batch(:likes, messages, _batch_info, _context) do
    messages
    |> Enum.map(fn %Message{data: like} -> like.subject end)
    |> Enum.reject(&is_nil/1)
    |> Stats.ingest_likes()

    messages
  end

  def handle_batch(:reposts, messages, _batch_info, _context) do
    messages
    |> Enum.map(fn %Message{data: repost} -> repost.subject end)
    |> Enum.reject(&is_nil/1)
    |> Stats.ingest_reposts()

    messages
  end

  # Everything else (deletes, identity and account events) just flows through.
  def handle_batch(:default, messages, _batch_info, _context), do: messages

  defp route(message, %{"kind" => "commit", "commit" => commit}) do
    case {Map.get(commit, "collection"), Map.get(commit, "operation")} do
      {"app.bsky.feed.post", "create"} ->
        message
        |> Message.put_batcher(:posts)
        |> Message.put_batch_key(language(commit))
        |> Message.update_data(fn _data -> normalize_post(commit) end)

      {"app.bsky.feed.like", "create"} ->
        message
        |> Message.put_batcher(:likes)
        |> Message.update_data(fn _data -> normalize_interaction(commit) end)

      {"app.bsky.feed.repost", "create"} ->
        message
        |> Message.put_batcher(:reposts)
        |> Message.update_data(fn _data -> normalize_interaction(commit) end)

      {_collection, _operation} ->
        message
    end
  end

  defp route(message, _event), do: message

  defp normalize_post(commit) do
    record = Map.get(commit, "record", %{})

    %{
      language: language(commit),
      hashtags: hashtags(record),
      reply?: is_map(Map.get(record, "reply"))
    }
  end

  defp normalize_interaction(commit) do
    %{
      subject:
        commit
        |> Map.get("record", %{})
        |> Map.get("subject", %{})
        |> Map.get("uri")
    }
  end

  defp language(commit) do
    case get_in(commit, ["record", "langs"]) do
      [language | _] -> language
      _other -> "unknown"
    end
  end

  defp hashtags(record) do
    facet_tags =
      record
      |> Map.get("facets", [])
      |> Enum.flat_map(fn facet -> Map.get(facet, "features", []) end)
      |> Enum.flat_map(fn
        %{"$type" => "app.bsky.richtext.facet#tag", "tag" => tag} -> [tag]
        _other -> []
      end)

    record
    |> Map.get("tags", [])
    |> Enum.filter(&is_binary/1)
    |> Enum.concat(facet_tags)
    |> Enum.map(&String.downcase/1)
    |> Enum.uniq()
  end

  defp summarize_posts(posts) do
    Enum.reduce(
      posts,
      %{total: 0, languages: %{}, hashtags: %{}, replies: 0},
      fn post, summary ->
        %{
          summary
          | total: summary.total + 1,
            languages: Map.update(summary.languages, post.language, 1, &(&1 + 1)),
            hashtags: Enum.reduce(post.hashtags, summary.hashtags, &Map.update(&2, &1, 1, fn c -> c + 1 end)),
            replies: summary.replies + if(post.reply?, do: 1, else: 0)
        }
      end
    )
  end
end

Logger.configure(level: :info)

children = [
  {Pulsar.Client,
   [
     name: :default,
     host: "pulsar://localhost:6650",
     producers: [[topic: "persistent://public/default/atproto-feed", name: "atproto-producer"]]
   ]},
  ATProto.Stats,
  ATProto.Pipeline,
  ATProto.FeedClient
]

{:ok, _pid} = Supervisor.start_link(children, strategy: :one_for_one, name: ATProto.Supervisor)
Process.sleep(:infinity)
