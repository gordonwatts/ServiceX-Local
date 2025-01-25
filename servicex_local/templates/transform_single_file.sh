#!/usr/bin/env bash
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# If transformer has already run then we don't need to compile
if [ ! -d /home/atlas/rel ]; then
  echo "Compile"
  bash --login "$SCRIPT_DIR/runner.sh" -c
  exit_code=$?
  if [ $exit_code != 0 ]; then
    echo "Compile step failed: $exit_code"
    exit $exit_code
  fi
fi

echo "Transform a file $1 -> $2"
bash --login "$SCRIPT_DIR/runner.sh" -r -d "$1" -o "$2"
exit_code=$?
if [ $exit_code != 0 ]; then
  echo "Transform step failed: $exit_code"
  exit $exit_code
fi
