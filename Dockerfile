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
# STAGE 1: RUST BUILDER
# =============================================================================
# Using nightly to support dependencies requiring rustc 1.88+
FROM rustlang/rust:nightly-bookworm AS rust-builder

WORKDIR /app

# Copy Cargo files first for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release && rm -rf src

# Copy actual source code and migrations (needed for include_str!)
COPY src ./src
COPY migrations ./migrations

# Touch main.rs to ensure it gets rebuilt
RUN touch src/main.rs

# Build the actual application
RUN cargo build --release

# =============================================================================
# STAGE 2: FRONTEND BUILDER
# =============================================================================
FROM node:22-alpine AS frontend-builder

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy package files first for dependency caching
COPY web/package.json web/pnpm-lock.yaml ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source code
COPY web/ ./

# Build the frontend
ARG VITE_CLERK_PUBLISHABLE_KEY=""
ARG VITE_API_URL="/api"
ENV VITE_CLERK_PUBLISHABLE_KEY=${VITE_CLERK_PUBLISHABLE_KEY}
ENV VITE_API_URL=${VITE_API_URL}

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

# Copy requirements and install dependencies
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# =============================================================================
# STAGE 4: COMPUTE-NODE (RUST RUNTIME)
# =============================================================================
FROM debian:bookworm-slim AS compute-node

LABEL maintainer="Rosetta Team"
LABEL description="Rosetta Compute Node - Rust Pipeline Manager"

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy built binary from rust-builder
COPY --from=rust-builder /app/target/release/rosetta /app/rosetta

# Copy assets (fonts)
COPY assets ./assets

# Set environment variables
ENV MODE=worker
ENV RUST_LOG=info
ENV TZ=Asia/Jakarta

# Create non-root user for security
RUN useradd -m -u 1000 rosetta && chown -R rosetta:rosetta /app
USER rosetta

# Run the application
CMD ["./rosetta"]

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

# Copy Python dependencies from backend-deps
COPY --from=backend-deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=backend-deps /usr/local/bin /usr/local/bin

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
