#!/usr/bin/env sh
# Install the latest isheika release into ~/.local/bin (override with
# ISHEIKA_BIN_DIR=/some/path).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/omnipin/isheika/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/omnipin/isheika/main/install.sh | ISHEIKA_VERSION=v0.1.0 sh
#
# Picks the right prebuilt tarball for your OS + CPU from the GitHub
# releases page. Falls back with a helpful message on unsupported
# platforms so you can build from source.

set -eu

REPO=${ISHEIKA_REPO:-omnipin/isheika}
BIN_DIR=${ISHEIKA_BIN_DIR:-$HOME/.local/bin}

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)        TARGET=x86_64-unknown-linux-gnu ;;
  Linux-aarch64)       TARGET=aarch64-unknown-linux-gnu ;;
  Darwin-x86_64)       TARGET=x86_64-apple-darwin ;;
  Darwin-arm64)        TARGET=aarch64-apple-darwin ;;
  *)
    echo "no prebuilt binary for $(uname -s)-$(uname -m)" >&2
    echo "build from source with: cargo install --git https://github.com/${REPO}" >&2
    exit 1
    ;;
esac

# Resolve the version: explicit override > GitHub's "latest" redirect.
if [ -n "${ISHEIKA_VERSION:-}" ]; then
  VERSION=$ISHEIKA_VERSION
else
  VERSION=$(curl -fsSLI -o /dev/null -w "%{url_effective}" \
    "https://github.com/${REPO}/releases/latest" | sed 's|.*/||')
fi

ASSET=isheika-${VERSION}-${TARGET}.tar.gz
URL=https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}

mkdir -p "$BIN_DIR"
echo "fetching ${URL}" >&2
# --strip-components=1 drops the tarball's top-level directory so we
# extract just the binary (and any other top-level files) into BIN_DIR.
curl -fsSL "$URL" | tar -xz -C "$BIN_DIR" --strip-components=1 \
  "isheika-${VERSION}-${TARGET}/isheika"

echo "installed ${BIN_DIR}/isheika" >&2
case ":$PATH:" in
  *:"$BIN_DIR":*) ;;
  *) echo "note: ${BIN_DIR} is not on \$PATH — add it to your shell profile" >&2 ;;
esac
"$BIN_DIR/isheika" --version
