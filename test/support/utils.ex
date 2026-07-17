defmodule OffBroadwayPulsar.Test.Support.Utils do
  @moduledoc false

  def wait_for(_fun, attempts \\ 100, interval_ms \\ 100)
  def wait_for(_fun, 0, _interval_ms), do: :error

  def wait_for(fun, attempts, interval_ms) do
    if fun.() do
      :ok
    else
      Process.sleep(interval_ms)
      wait_for(fun, attempts - 1, interval_ms)
    end
  end

  def notify_active_state(metadata, test_pid, tag) do
    send(test_pid, {:active_state_callback, metadata, tag})
  end
end
