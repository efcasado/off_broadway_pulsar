OffBroadwayPulsar.Test.Support.System.start_pulsar()

Logger.configure(level: :info)

Application.put_env(:junit_formatter, :report_dir, "test/reports")
Application.put_env(:junit_formatter, :report_file, "junit.xml")
Application.put_env(:junit_formatter, :automatic_create_dir?, true)

ExUnit.start(formatters: [JUnitFormatter, ExUnit.CLIFormatter])

ExUnit.after_suite(fn _ ->
  OffBroadwayPulsar.Test.Support.System.stop_pulsar()
end)
