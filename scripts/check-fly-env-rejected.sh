#!/usr/bin/env bash
# Fails if any fly/*.toml [env] block sets an environment variable that
# crates/indexer's Config::from_env (or another service's config loader)
# now hard-rejects at startup (issue #777). POLL_INTERVAL_MS shipped exactly
# this way: fly/indexer.toml kept setting it long after the indexer started
# refusing to boot with it set, and nothing caught the mismatch until a real
# deploy crash-looped.
#
# This is a grep-based check, in the same spirit as
# scripts/check-env-reference.sh: it extracts the rejected-var names
# straight from the Rust source (so a newly-rejected var is caught
# automatically, with no second list to keep in sync) and greps every
# fly/*.toml [env] block for them.
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG_RS="crates/indexer/src/config.rs"

# Vars rejected unconditionally via `if std::env::var("X").is_ok() { errors.push(...) }`.
mapfile -t single_rejected < <(
  grep -oE 'std::env::var\("[A-Z][A-Z0-9_]*"\)\.is_ok\(\)' "$CONFIG_RS" \
    | grep -oE '"[A-Z][A-Z0-9_]*"' | tr -d '"' | sort -u
)

# Vars rejected via the `for (key, replacement) in [("X", "Y"), ...]` loop.
mapfile -t tuple_rejected < <(
  awk '/for \(key, replacement\) in \[/{inside=1; next} inside && /\]/{exit} inside' "$CONFIG_RS" \
    | grep -oE '\("[A-Z][A-Z0-9_]*"' | grep -oE '"[A-Z][A-Z0-9_]*"' | tr -d '"' | sort -u
)

rejected_vars=("${single_rejected[@]}" "${tuple_rejected[@]}")

if [ "${#rejected_vars[@]}" -eq 0 ]; then
  echo "No rejected env vars found in $CONFIG_RS — check the grep patterns still match." >&2
  exit 1
fi

fail=0
for toml in fly/*.toml; do
  [ -f "$toml" ] || continue
  # Only look inside the [env] block, not comments describing secrets.
  env_block=$(awk '/^\[env\]/{inside=1; next} /^\[/{inside=0} inside' "$toml")
  for var in "${rejected_vars[@]}"; do
    if echo "$env_block" | grep -qE "^\s*${var}\s*="; then
      echo "::error file=$toml::$var is set in [env] but is rejected at startup by $CONFIG_RS — see the matching startup-validation error message for its replacement." >&2
      fail=1
    fi
  done
done

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "Remove the rejected var(s) above from the Fly config(s) listed, using the replacement named in each startup error in $CONFIG_RS." >&2
  exit 1
fi

echo "No fly/*.toml [env] block sets a rejected env var."
