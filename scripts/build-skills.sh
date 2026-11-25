#!/usr/bin/env bash
# Assemble the packaged skills and standalone Hosted guide.
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "$0")/.." && pwd)"
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 OUTPUT_DIRECTORY" >&2
  exit 2
fi
output_dir="$1"
mkdir -p "$output_dir"
version="$(sed -nE 's/^[[:space:]]*version: "([0-9]+\.[0-9]+\.[0-9]+)",$/\1/p' "$repo_dir/mix.exs")"
if [[ -z "$version" ]]; then
  echo "Cannot read the Server version from mix.exs." >&2
  exit 1
fi
for skill in harklight harklight-agent harklight-agent-saas; do
  mkdir -p "$output_dir/$skill"
  {
    if [[ "$skill" == harklight ]]; then
      sed "s/@SERVER_VERSION@/$version/g" "$repo_dir/skills/$skill/SKILL.md.in"
    else
      cat "$repo_dir/skills/$skill/SKILL.md.in"
    fi
    if [[ "$skill" != harklight-agent-saas ]]; then
      printf "\n"
      cat "$repo_dir/skills/core.md"
    fi
  } > "$output_dir/$skill/SKILL.md"
done
install -m 644 "$repo_dir/skills/core.md" "$output_dir/core.md"
