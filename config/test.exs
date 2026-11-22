import Config

# Every keyed commit in tests proves the per-address verdict equals the
# whole-state verdict.
config :attention_server, keyed_validation: :both
