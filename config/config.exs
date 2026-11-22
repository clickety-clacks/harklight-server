import Config

# Keyed commits use per-address validation by default. :full checks the whole
# state, and :both runs both validators and raises when they disagree.
# Temporary; see docs/feature-flags.md.
config :attention_server, keyed_validation: :incremental

if config_env() == :test, do: import_config("test.exs")
