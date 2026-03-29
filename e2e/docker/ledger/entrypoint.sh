#!/usr/bin/env bash
set -euo pipefail

extra_args=""

# Load the serviceability program at genesis.
if [ -n "${SERVICEABILITY_PROGRAM_ID:-}" ]; then
  extra_args="${extra_args} --bpf-program ${SERVICEABILITY_PROGRAM_ID} /programs/doublezero_serviceability.so"
  echo "==> Loading serviceability program: ${SERVICEABILITY_PROGRAM_ID}"
fi

echo "==> Starting solana-test-validator"

# Start the validator. Filter noisy "Processed Slot:" messages.
script -q -c "solana-test-validator ${extra_args} 2>&1" /dev/null | grep --line-buffered -v "Processed Slot: "
