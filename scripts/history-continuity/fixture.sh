#!/usr/bin/env bash
set -euo pipefail
umask 077

action="${1:?expected init, upgrade, or serve}"
root="$(realpath -m "${2:?expected disposable fixture directory}")"
package="$(realpath "${3:?expected extracted package directory}")"
script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
release_value() {
  sed -n "s/^$2=//p" "$1/BUILD_INFO"
}
version="$(release_value "$package" version)"
commit="$(release_value "$package" source_commit)"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$commit" =~ ^[0-9a-f]{40}$ ]]
state="$root/state"

inventory() {
  ATTENTION_SERVER_ENABLE=0 FIXTURE_DB="$state/relay.sqlite3" \
    STAGE="$version" OWNER_DER="$root/owner.der" INVENTORY="$1" HISTORY_SEED="$2" \
    FIXTURE_SCRIPT="$script_dir/inventory.exs" \
    "$package/bin/attention_server" eval 'Code.require_file(System.fetch_env!("FIXTURE_SCRIPT"))'
}

case "$action" in
  init)
    [[ "$version" == 0.1.49 ]]
    mkdir -- "$root"
    mkdir -- "$root/results"
    "$package/bin/harklight-bootstrap" "$state" "${4:-localhost}"
    openssl x509 -in "$state/owner/identity.pem" -outform DER -out "$root/owner.der"
    inventory "$root/results/predecessor.json" yes
    touch "$root/.harklight-history-fixture"
    ;;
  upgrade)
    [[ -f "$root/.harklight-history-fixture" && ! -e "$root/pre-upgrade" ]]
    bridge="$(realpath "${4:?expected extracted FORMAT30 bridge package}")"
    [[ "$(release_value "$bridge" version)" == 0.2.16 ]]
    predecessor="$(sha256sum "$state/relay.sqlite3" | cut -d' ' -f1)"
    cp -a -- "$state" "$root/pre-upgrade"
    ATTENTION_SERVER_ENABLE=0 FIXTURE_DB="$state/relay.sqlite3" \
      "$bridge/bin/attention_server" eval '
        {:ok, _} = Application.ensure_all_started(:attention_server)
        {:ok, relay} = AttentionServer.Relay.start_link(store_path: System.fetch_env!("FIXTURE_DB"))
        :ok = GenServer.stop(relay)
      '
    "$package/bin/harklight" store convert-stopped "$state/relay.sqlite3" --from-release v0.2.16
    inventory "$root/results/target.json" no
    preserved="$(sha256sum "$root/pre-upgrade/relay.sqlite3" | cut -d' ' -f1)"
    [[ "$predecessor" == "$preserved" ]]
    python3 "$script_dir/verify.py" inventory "$root/results" "$version" "$commit"
    ;;
  serve)
    [[ -f "$root/.harklight-history-fixture" ]]
    source "$state/server.env"
    exec "$package/bin/attention_server" start
    ;;
  *)
    echo "unknown fixture action: $action" >&2
    exit 2
    ;;
esac
