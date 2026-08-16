defmodule OffBroadway.Pulsar.AcknowledgerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias OffBroadway.Pulsar.Acknowledger
  alias OffBroadwayPulsar.Test.Support.StubConsumer

  defp message(consumer, id) do
    %Broadway.Message{
      data: "payload",
      acknowledger: {Acknowledger, %{consumer: consumer}, %{ledgerId: 1, entryId: id}}
    }
  end

  defp dead_consumer do
    {:ok, consumer} = StubConsumer.start_link(self())
    Process.unlink(consumer)
    ref = Process.monitor(consumer)
    Process.exit(consumer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^consumer, :killed}

    consumer
  end

  test "acknowledges successful and failed messages against the delivering worker" do
    {:ok, consumer} = start_supervised({StubConsumer, self()})

    assert :ok = Acknowledger.ack(%{consumer: consumer}, [message(consumer, 1)], [message(consumer, 2)])

    assert_receive {:ack, [%{entryId: 1}]}
    assert_receive {:nack, [%{entryId: 2}]}
  end

  test "does not call the worker when there is nothing to acknowledge" do
    {:ok, consumer} = start_supervised({StubConsumer, self()})

    assert :ok = Acknowledger.ack(%{consumer: consumer}, [message(consumer, 1)], [])

    assert_receive {:ack, [%{entryId: 1}]}
    refute_receive {:nack, _ids}
  end

  test "drops the acknowledgements of a worker that has been replaced" do
    consumer = dead_consumer()

    log =
      capture_log(fn ->
        assert :ok = Acknowledger.ack(%{consumer: consumer}, [message(consumer, 1)], [message(consumer, 2)])
      end)

    assert log =~ "Dropping ack of 1 message(s)"
    assert log =~ "Dropping nack of 1 message(s)"
  end

  test "a replaced worker does not abort the acknowledgements owed to its live siblings" do
    # Broadway acknowledges one worker at a time, in key order, and stops at the first exit;
    # the older pid sorts first, so the dead worker is the one reached first.
    dead = dead_consumer()
    {:ok, live} = start_supervised({StubConsumer, self()})

    capture_log(fn ->
      Broadway.Acknowledger.ack_messages([message(dead, 1), message(live, 2)], [])
    end)

    assert_receive {:ack, [%{entryId: 2}]}
  end

  test "logs an error reply instead of failing the batch" do
    {:ok, consumer} = start_supervised({StubConsumer, {self(), {:error, :not_connected}}})

    log =
      capture_log(fn ->
        assert :ok = Acknowledger.ack(%{consumer: consumer}, [message(consumer, 1)], [])
      end)

    assert log =~ "Pulsar ack of 1 message(s) failed: :not_connected"
  end
end
