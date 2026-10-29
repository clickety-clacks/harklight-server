#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repo_dir"
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo "This package supports Linux x86_64; build on the declared target." >&2
  exit 2
fi
version=0.1.19
dist_dir="$repo_dir/dist"
if [[ $# -gt 0 ]]; then dist_dir=$1; fi
mkdir -p "$dist_dir"
dist_dir="$(cd "$dist_dir" && pwd)"
export MIX_ENV=prod ATTENTION_SERVER_ENABLE=0
mix deps.get --only prod
stage="$(mktemp -d)"
trap 'rm -rf -- "$stage"' EXIT
name="harklight-server-$version-linux-x86_64"
mix release attention_server --path "$stage/$name" --overwrite
install -m 755 scripts/harklight "$stage/$name/bin/harklight"
install -m 755 scripts/bootstrap.sh "$stage/$name/bin/harklight-bootstrap"
install -m 755 scripts/install-release.sh "$stage/$name/bin/harklight-release-install"
install -m 644 SELF_HOST.md "$stage/$name/SELF_HOST.md"
install -m 644 GUIDED_JOIN.md "$stage/$name/GUIDED_JOIN.md"
mkdir -p "$stage/$name/skills/harklight-agent"
install -m 644 skills/harklight-agent/SKILL.md "$stage/$name/skills/harklight-agent/SKILL.md"
mkdir -p "$stage/$name/skills/harklight-agent-send-listen"
install -m 644 skills/harklight-agent-send-listen/SKILL.md "$stage/$name/skills/harklight-agent-send-listen/SKILL.md"
cp -a third_party "$stage/$name/THIRD_PARTY"
for application in "$stage/$name"/lib/*; do
  component="$(basename "$application")"
  dependency="$(printf '%s' "$component" | sed 's/-[^-]*$//')"
  for notice in LICENSE LICENSE.txt NOTICE COPYING; do
    if [[ -f "deps/$dependency/$notice" ]]; then
      mkdir -p "$stage/$name/THIRD_PARTY/$component"
      install -m 644 "deps/$dependency/$notice" "$stage/$name/THIRD_PARTY/$component/$notice"
    fi
  done
done
{
  printf 'product=Harklight Server\nversion=%s\n' "$version"
  printf 'source_commit=%s\n' "$(git rev-parse HEAD)"
  printf 'source_status=%s\n' "$(git status --porcelain)"
  printf 'build_system=%s\n' "$(uname -srvmo)"
  mix --version
  printf 'native_dependencies\n'
  ldd "$stage/$name"/erts-*/bin/beam.smp
  ldd "$stage/$name"/lib/exqlite-*/priv/sqlite3_nif.so
} > "$stage/$name/BUILD_INFO"
epoch="$(git show -s --format=%ct HEAD)"
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner \
  -C "$stage" -cf - "$name" | gzip -n > "$dist_dir/$name.tar.gz"
(cd "$dist_dir" && sha256sum "$name.tar.gz" > SHA256SUMS)
printf '%s\n' "$dist_dir/$name.tar.gz"
