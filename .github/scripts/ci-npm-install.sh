#!/usr/bin/env bash
# MemoryCore npm bootstrap for GitHub Actions.
#
# Root .gitignore lists package-lock.json (not shipped in MRs), so this tree
# usually has no MemoryCore lockfile. Prefer `npm ci` when one is present.
set -euo pipefail

echo "node $(node -v); npm $(npm -v)"

if [ -f package-lock.json ]; then
  echo "lockfile present → npm ci --ignore-scripts"
  npm ci --ignore-scripts
else
  echo "no package-lock.json → npm install --ignore-scripts"
  # Fresh trees on npm 10.x can hit arborist "Cannot read properties of null
  # (reading 'edgesOut')" on this optional-peer graph; retry without peers.
  if ! npm install --ignore-scripts; then
    echo "npm install failed; retrying with --legacy-peer-deps"
    npm install --ignore-scripts --legacy-peer-deps
  fi
fi
