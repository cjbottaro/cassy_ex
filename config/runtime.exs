import Config

if config_env() == :test do
  Logger.put_application_level(:cassandra_ex, :info)
  Logger.put_application_level(:telemetry, :warning)
end
