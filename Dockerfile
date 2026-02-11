# =============================================================================
# ROSETTA MULTI-STAGE DOCKERFILE
# =============================================================================
# This Dockerfile builds 3 applications:
# 1. Rust compute-node (worker)
# 2. FastAPI backend
# 3. React frontend
#
# Final images:
# - compute-node: Rust application (mode=worker)
# - web: FastAPI + React served via Nginx (mode=web)
# =============================================================================

# =============================================================================
# STAGE 1: COMPUTE DEPENDENCIES (PYTHON)
# =============================================================================
FROM python:3.11-slim-bookworm AS compute-deps

WORKDIR /app

# Install system dependencies
# git: for removing git+ dependencies if needed, or installing from git
# gcc, libpq-dev: for psycopg2
# default-jre-headless: for JPype1 (Debezium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    git \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy pyproject.toml and uv.lock
COPY compute/pyproject.toml compute/uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-install-project


# =============================================================================
# STAGE 2: FRONTEND BUILDER
# =============================================================================
FROM node:22-alpine AS frontend-builder

WORKDIR /app

# Install build dependencies needed for native modules
RUN apk add --no-cache python3 make g++

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy package files first for dependency caching
COPY web/package.json ./

# Install dependencies (without lock file, will resolve fresh)
RUN pnpm install --no-frozen-lockfile

# Copy source code
COPY web/ ./

# Build the frontend
# Note: VITE_API_URL is left empty so client.ts uses window.location.origin dynamically
ARG VITE_CLERK_PUBLISHABLE_KEY=""
ENV VITE_CLERK_PUBLISHABLE_KEY=${VITE_CLERK_PUBLISHABLE_KEY}

RUN pnpm build

# =============================================================================
# STAGE 3: BACKEND DEPENDENCIES
# =============================================================================
FROM python:3.11-slim-bookworm AS backend-deps

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy uv lock file and pyproject.toml
COPY backend/pyproject.toml backend/uv.lock ./

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install dependencies
RUN uv sync --frozen --no-install-project

# =============================================================================
# STAGE 4: COMPUTE-NODE (PYTHON RUNTIME)
# =============================================================================
FROM python:3.11-slim-bookworm AS compute-node

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Compute Node - Python Pipeline Manager"

# Install runtime dependencies
# libpq5: for psycopg2
# default-jre-headless: for JPype1 (Debezium)
# curl: for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from compute-deps
COPY --from=compute-deps /app/.venv /app/.venv

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy compute source code
COPY compute/ ./compute/

# Copy migrations
COPY migrations ./migrations

# Set environment variables
ENV MODE=worker
ENV PYTHONPATH=/app/compute
ENV TZ=Asia/Jakarta

# Create non-root user for security
RUN useradd -m -u 1000 rosetta && chown -R rosetta:rosetta /app
USER rosetta

# Run the application
CMD ["python", "-m", "compute.main"]

# Expose API port
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# =============================================================================
# STAGE 5: WEB (BACKEND + FRONTEND)
# =============================================================================
FROM python:3.11-slim-bookworm AS web

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Web - FastAPI Backend + React Frontend"

# Install runtime dependencies and nginx
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    supervisor \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment
COPY --from=backend-deps /app/.venv /app/.venv

# Enable virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy backend source code
COPY backend/ ./backend/

# Copy built frontend from frontend-builder
COPY --from=frontend-builder /app/dist /var/www/html

# Copy nginx configuration
COPY docker/nginx.conf /etc/nginx/nginx.conf

# Copy supervisor configuration
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy entrypoint script
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set environment variables
ENV MODE=web
ENV HOST=0.0.0.0
ENV PORT=8000
ENV TZ=Asia/Jakarta
ENV PYTHONPATH=/app/backend

# Expose ports
EXPOSE 80 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run entrypoint
CMD ["/app/entrypoint.sh"]
