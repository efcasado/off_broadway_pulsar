defmodule OffBroadwayPulsar.Test.Support.System do
  @moduledoc """
  Helper functions for managing the test environment (i.e. Pulsar).
  """

  def start_pulsar do
    IO.puts("Starting Pulsar...")
    {_output, 0} = System.cmd("docker", ["compose", "up", "-d"])

    IO.puts("Waiting for Pulsar to become healthy...")
    wait_for_pulsar()
  end

  def stop_pulsar do
    IO.puts("Stopping Pulsar...")
    {_output, 0} = System.cmd("docker", ["compose", "down", "-v"])
  end

  @doc """
  Creates a non-partitioned Pulsar topic via pulsar-admin inside the running container.
  Idempotent: succeeds even if the topic already exists.
  """
  def create_topic(topic) do
    {_output, _exit_code} =
      System.cmd(
        "docker",
        ["exec", "pulsar", "bin/pulsar-admin", "topics", "create", topic],
        stderr_to_stdout: true
      )

    :ok
  end

  @doc """
  Creates a partitioned Pulsar topic with the given number of partitions.
  """
  def create_partitioned_topic(topic, partitions) do
    {_output, 0} =
      System.cmd(
        "docker",
        [
          "exec",
          "pulsar",
          "bin/pulsar-admin",
          "topics",
          "create-partitioned-topic",
          topic,
          "--partitions",
          "#{partitions}"
        ],
        stderr_to_stdout: true
      )

    :ok
  end

  @doc """
  Updates the partition count of an existing partitioned topic.
  The new count must be greater than the current count.
  """
  def update_partitions(topic, partitions) do
    {_output, 0} =
      System.cmd(
        "docker",
        [
          "exec",
          "pulsar",
          "bin/pulsar-admin",
          "topics",
          "update-partitioned-topic",
          topic,
          "--partitions",
          "#{partitions}"
        ],
        stderr_to_stdout: true
      )

    :ok
  end

  defp wait_for_pulsar(retries \\ 30) do
    if retries == 0 do
      raise "Pulsar failed to become healthy"
    end

    case System.cmd("curl", ["-f", "http://localhost:8080/metrics/health"], stderr_to_stdout: true) do
      {_output, 0} ->
        IO.puts("Pulsar is healthy!")
        :ok

      {_output, _code} ->
        Process.sleep(1000)
        wait_for_pulsar(retries - 1)
    end
  end
end
