#!/bin/bash
set -e

echo "=================================="
echo "  Rosetta Web Container Starting  "
echo "=================================="
echo "MODE: ${MODE}"
echo "=================================="

# Create log directories
mkdir -p /var/log/supervisor
mkdir -p /var/log/nginx

# Run database migrations if needed
if [ "${RUN_MIGRATIONS:-false}" = "true" ]; then
    echo "Running database migrations..."
    cd /app/backend
    alembic upgrade head
    echo "Migrations completed."
fi

# Start supervisord (manages nginx + uvicorn)
echo "Starting supervisord..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf
