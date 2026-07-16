#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
GO=${GO:-go}

if [ -z "${SFS_SUBDIR_TEST_RENDERED_CONFIG:-}" ]; then
  echo "rendered runtime config path is required" >&2
  exit 1
fi

cd "$ROOT_DIR"
exec "$GO" test ./pkg/config ./internal/driverapp -run '^TestRenderedRuntimeConfigFromHelm$' -count=1
