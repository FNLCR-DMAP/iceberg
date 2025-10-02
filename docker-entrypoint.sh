#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] Apache Iceberg ETL Demo"

# If a custom jar is mounted/provided, prefer it
if [ -n "${ICEBERG_JAR:-}" ]; then
  if [ -f "$ICEBERG_JAR" ]; then
    echo "[entrypoint] Using ICEBERG_JAR env override: $ICEBERG_JAR"
  else
    echo "[entrypoint] WARNING: ICEBERG_JAR env var set but file missing: $ICEBERG_JAR" >&2
  fi
fi

exec "$@"
