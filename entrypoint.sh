#!/bin/bash
set -e

DUMP_FILE="/data/meridian_bank.dump"
SENTINEL="/var/lib/postgresql/data/.restore_done"
DB_NAME="meridian_bank"
DB_USER="meridian"

# --- Start Postgres in background ---
docker-entrypoint.sh postgres &
PG_PID=$!

# Wait for Postgres to be ready
echo "Waiting for PostgreSQL to start..."
until pg_isready -U "$DB_USER" -d "$DB_NAME" -q; do
    sleep 1
done
echo "PostgreSQL is ready."

# --- Restore dump on first boot ---
if [ ! -f "$SENTINEL" ] && [ -f "$DUMP_FILE" ]; then
    echo "First boot: restoring $DUMP_FILE..."
    pg_restore -U "$DB_USER" -d "$DB_NAME" \
        --no-owner --no-privileges --jobs=2 \
        "$DUMP_FILE" || true  # ignore non-fatal errors
    touch "$SENTINEL"
    echo "Restore complete."
elif [ -f "$SENTINEL" ]; then
    echo "Database already restored (sentinel exists). Skipping."
else
    echo "WARNING: No dump file found at $DUMP_FILE"
fi

# --- Start MCP server ---
echo "Starting MCP server (read-only, SSE transport on 0.0.0.0:8000)..."
DATABASE_URI="postgresql://${DB_USER}:${POSTGRES_PASSWORD}@localhost:5432/${DB_NAME}" \
    postgres-mcp --access-mode=restricted --transport=sse --sse-host 0.0.0.0 &
MCP_PID=$!

echo "Meridian Bank ready:"
echo "  PostgreSQL: localhost:5432 (db=$DB_NAME user=$DB_USER)"
echo "  MCP (SSE):  http://localhost:8000/sse"

# --- Wait for either process to exit ---
wait -n $PG_PID $MCP_PID
EXIT_CODE=$?
echo "A process exited with code $EXIT_CODE. Shutting down..."
kill $PG_PID $MCP_PID 2>/dev/null || true
wait
exit $EXIT_CODE
