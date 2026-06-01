#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 {start|exit}" >&2
  exit 2
}

if [[ $# -ne 1 ]]; then
  usage
fi

case "$1" in
  start)
    label="START GATE"
    ;;
  exit)
    label="EXIT GATE"
    ;;
  *)
    echo "Unknown gate action: $1" >&2
    usage
    ;;
esac

echo "===== ${label} ====="
echo "Branch:"
git branch --show-current
echo
echo "Status:"
git status --short
