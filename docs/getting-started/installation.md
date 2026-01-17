# Installation

This guide covers installing FLEET-Q in various environments.

## Requirements

- **Python**: 3.9 or higher
- **Snowflake**: Account with appropriate permissions
- **Container Runtime**: Docker (for containerized deployments)

## PyPI Installation

FLEET-Q is available on PyPI and can be installed using pip:

```bash
pip install fleet-q
```

### Development Installation

For development work, install with development dependencies:

```bash
pip install fleet-q[dev]
```

This includes testing, linting, and documentation tools.

## Container Installation

FLEET-Q is designed to run in containers and provides official Docker images:

```bash
# Pull the latest image
docker pull fleet-q:latest

# Or build from source
git clone https://github.com/fleet-q/fleet-q
cd fleet-q
docker build -t fleet-q .
```

## Source Installation

To install from source using uv (recommended for development):

```bash
# Clone the repository
git clone https://github.com/fleet-q/fleet-q
cd fleet-q

# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install development dependencies
uv sync --extra dev

# Install documentation dependencies
uv sync --extra docs
```

## Verification

Verify your installation:

```bash
# Check version
fleet-q --version

# Or using Python module
python -m fleet_q --version
```

## Database Setup

FLEET-Q requires two tables in your Snowflake database:

### POD_HEALTH Table

```sql
CREATE TABLE POD_HEALTH (
    pod_id STRING PRIMARY KEY,
    birth_ts TIMESTAMP_NTZ NOT NULL,
    last_heartbeat_ts TIMESTAMP_NTZ NOT NULL,
    status STRING NOT NULL, -- 'up' | 'down'
    meta VARIANT, -- {cpu_count, thread_count, version, etc.}
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### STEP_TRACKER Table

```sql
CREATE TABLE STEP_TRACKER (
    step_id STRING PRIMARY KEY,
    status STRING NOT NULL, -- 'pending' | 'claimed' | 'completed' | 'failed'
    claimed_by STRING, -- pod_id
    last_update_ts TIMESTAMP_NTZ NOT NULL,
    payload VARIANT NOT NULL, -- {type, args, metadata}
    retry_count INTEGER DEFAULT 0,
    priority INTEGER DEFAULT 0,
    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    max_retries INTEGER DEFAULT 3
);
```

### Automatic Schema Creation

FLEET-Q can automatically create these tables on first run:

```bash
# Set environment variables (see Configuration guide)
export FLEET_Q_SNOWFLAKE_ACCOUNT="your-account"
export FLEET_Q_SNOWFLAKE_USER="your-user"
export FLEET_Q_SNOWFLAKE_PASSWORD="your-password"
export FLEET_Q_SNOWFLAKE_DATABASE="your-database"

# Run with schema creation
fleet-q --create-schema
```

## Permissions

Your Snowflake user needs the following permissions:

```sql
-- Database and schema access
GRANT USAGE ON DATABASE your_database TO ROLE your_role;
GRANT USAGE ON SCHEMA your_database.your_schema TO ROLE your_role;

-- Table operations
GRANT CREATE TABLE ON SCHEMA your_database.your_schema TO ROLE your_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA your_database.your_schema TO ROLE your_role;

-- Future tables (for automatic schema creation)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA your_database.your_schema TO ROLE your_role;
```

## Next Steps

Once installed, continue with:

- **[Quick Start](quick-start.md)** - Get FLEET-Q running
- **[Configuration](configuration.md)** - Configure for your environment
- **[Docker Guide](../operations/docker.md)** - Container deployment