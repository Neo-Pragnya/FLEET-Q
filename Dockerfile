# Multi-stage Docker build for FLEET-Q

FROM python:3.11-slim as builder

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies with uv
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install uv for runtime
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Create non-root user for security
RUN groupadd -r fleetq && useradd -r -g fleetq -s /bin/bash fleetq

# Set working directory
WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY fleet_q/ ./fleet_q/
COPY pyproject.toml ./

# Create directories for SQLite and logs with proper permissions
RUN mkdir -p /app/data /app/logs && \
    chown -R fleetq:fleetq /app

# Switch to non-root user
USER fleetq

# Set environment variables for single-process execution
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLEET_Q_SQLITE_DB_PATH="/app/data/fleet_q.db" \
    FLEET_Q_API_HOST="0.0.0.0" \
    FLEET_Q_API_PORT="8000"

# Health check configuration for container orchestration
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose the API port
EXPOSE 8000

# Use exec form for proper signal handling in containers
CMD ["python", "-m", "fleet_q.main"]