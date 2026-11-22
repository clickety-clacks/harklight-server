import Config

# Keyed commit validation: :full checks the whole state (production),
# :incremental checks only the touched addresses, and :both runs both and
# raises when they disagree. Temporary; see docs/feature-flags.md.
config :attention_server, keyed_validation: :full

if config_env() == :test, do: import_config("test.exs")
