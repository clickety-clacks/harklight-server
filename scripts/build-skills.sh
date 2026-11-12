#!/usr/bin/env bash
# Assemble three downloads; the reviewed SaaS guide is a complete document.
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "$0")/.." && pwd)"
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 OUTPUT_DIRECTORY" >&2
  exit 2
fi
output_dir="$1"
mkdir -p "$output_dir"
for skill in harklight-agent harklight-agent-saas; do
  mkdir -p "$output_dir/$skill"
  {
    cat "$repo_dir/skills/$skill/SKILL.md.in"
    if [[ "$skill" == harklight-agent ]]; then
      printf "\n"
      cat "$repo_dir/skills/core.md"
    fi
  } > "$output_dir/$skill/SKILL.md"
done
install -m 644 "$repo_dir/skills/core.md" "$output_dir/core.md"
