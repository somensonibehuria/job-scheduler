#!/bin/sh
#
# Minimal Maven Wrapper shim.
# Uses system Maven if present, otherwise instructs to install Maven.

if command -v mvn >/dev/null 2>&1; then
  exec mvn "$@"
fi

echo "mvn not found. Please install Maven (mvn) and re-run." >&2
exit 1

