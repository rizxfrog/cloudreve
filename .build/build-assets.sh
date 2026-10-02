#!/bin/bash
# Build the frontend assets and pack them into application/statics/assets.zip,
# which is embedded into the backend binary via go:embed.
#
# Usage: .build/build-assets.sh <version>
# If <version> is omitted, the version currently set in frontend/package.json is used.
set -e

export NODE_OPTIONS="--max-old-space-size=8192"

VERSION="${1:-}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/frontend"

rm -rf build/*

# Prefer the lockfile-pinned install when available, fall back to yarn/npm.
HAS_BUN=false
command -v bun >/dev/null 2>&1 && HAS_BUN=true
if [ "$HAS_BUN" = true ]; then
    bun install
    [ -n "$VERSION" ] && bun pm pkg set "version=$VERSION"
    bun run build
else
    yarn install --network-timeout 1000000
    [ -n "$VERSION" ] && yarn version --new-version "$VERSION" --no-git-tag-version
    yarn run build
fi

# The embed directive reads "assets/build" out of the zip, so the archive must
# contain an "assets/build" prefix.
cd "$ROOT"
rm -f application/statics/assets.zip
rm -rf .tmp-assets
mkdir -p .tmp-assets/assets
cp -r frontend/build .tmp-assets/assets/build
(cd .tmp-assets && zip -rq "$ROOT/application/statics/assets.zip" assets/build)
rm -rf .tmp-assets

# A zip without the expected prefix packs fine but leaves the server with no
# index.html, so every non-API path answers a bare 404. Fail here instead of
# shipping a binary that looks healthy but serves nothing.
if ! unzip -l application/statics/assets.zip | grep -q "assets/build/index.html"; then
    echo "error: assets.zip is missing assets/build/index.html; check the archive prefix." >&2
    exit 1
fi

echo "Generated application/statics/assets.zip ($(du -h application/statics/assets.zip | cut -f1))"
