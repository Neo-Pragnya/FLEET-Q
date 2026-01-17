"""
FLEET-Q Configuration Management

Loads configuration from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration"""
    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: Optional[str] = None


@dataclass
class FleetQConfig:
    """Main FLEET-Q configuration"""
    
    # Pod identity
    pod_id: str
    
    # Snowflake connection
    snowflake: SnowflakeConfig
    
    # Table names
    pod_health_table: str = "POD_HEALTH"
    step_tracker_table: str = "STEP_TRACKER"
    
    # Timing intervals (seconds)
    heartbeat_interval: int = 10
    leader_check_interval: int = 15
    recovery_interval: int = 30
    claim_interval: int = 5
    
    # Capacity management
    max_parallelism: int = 8
    capacity_threshold: float = 0.8
    
    # Dead pod detection
    dead_pod_threshold_seconds: int = 60
    
    # Retry policy
    max_retries: int = 3
    
    # Backoff defaults
    backoff_base_delay_ms: int = 50
    backoff_max_delay_ms: int = 5000
    backoff_max_attempts: int = 5
    
    # Local SQLite path (for leader DLQ)
    local_db_path: str = "/tmp/fleet_q_local.db"


def load_config() -> FleetQConfig:
    """
    Load configuration from environment variables.
    
    Required environment variables:
    - FLEET_Q_POD_ID: Unique pod identifier
    - SNOWFLAKE_ACCOUNT: Snowflake account identifier
    - SNOWFLAKE_USER: Snowflake username
    - SNOWFLAKE_PASSWORD: Snowflake password
    - SNOWFLAKE_DATABASE: Database name
    - SNOWFLAKE_SCHEMA: Schema name
    - SNOWFLAKE_WAREHOUSE: Warehouse name
    
    Optional environment variables (with defaults):
    - SNOWFLAKE_ROLE: Snowflake role
    - FLEET_Q_POD_HEALTH_TABLE: POD_HEALTH (default)
    - FLEET_Q_STEP_TRACKER_TABLE: STEP_TRACKER (default)
    - FLEET_Q_HEARTBEAT_INTERVAL: 10 (seconds)
    - FLEET_Q_LEADER_CHECK_INTERVAL: 15 (seconds)
    - FLEET_Q_RECOVERY_INTERVAL: 30 (seconds)
    - FLEET_Q_CLAIM_INTERVAL: 5 (seconds)
    - FLEET_Q_MAX_PARALLELISM: 8
    - FLEET_Q_CAPACITY_THRESHOLD: 0.8
    - FLEET_Q_DEAD_POD_THRESHOLD: 60 (seconds)
    - FLEET_Q_MAX_RETRIES: 3
    - FLEET_Q_LOCAL_DB_PATH: /tmp/fleet_q_local.db
    """
    
    # Required fields
    pod_id = os.getenv("FLEET_Q_POD_ID")
    if not pod_id:
        raise ValueError("FLEET_Q_POD_ID environment variable is required")
    
    # Snowflake config
    snowflake_config = SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        database=os.getenv("SNOWFLAKE_DATABASE", ""),
        schema=os.getenv("SNOWFLAKE_SCHEMA", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    
    # Validate Snowflake config
    if not all([snowflake_config.account, snowflake_config.user, 
                snowflake_config.password, snowflake_config.database,
                snowflake_config.schema, snowflake_config.warehouse]):
        raise ValueError("All Snowflake connection parameters must be provided")
    
    return FleetQConfig(
        pod_id=pod_id,
        snowflake=snowflake_config,
        pod_health_table=os.getenv("FLEET_Q_POD_HEALTH_TABLE", "POD_HEALTH"),
        step_tracker_table=os.getenv("FLEET_Q_STEP_TRACKER_TABLE", "STEP_TRACKER"),
        heartbeat_interval=int(os.getenv("FLEET_Q_HEARTBEAT_INTERVAL", "10")),
        leader_check_interval=int(os.getenv("FLEET_Q_LEADER_CHECK_INTERVAL", "15")),
        recovery_interval=int(os.getenv("FLEET_Q_RECOVERY_INTERVAL", "30")),
        claim_interval=int(os.getenv("FLEET_Q_CLAIM_INTERVAL", "5")),
        max_parallelism=int(os.getenv("FLEET_Q_MAX_PARALLELISM", "8")),
        capacity_threshold=float(os.getenv("FLEET_Q_CAPACITY_THRESHOLD", "0.8")),
        dead_pod_threshold_seconds=int(os.getenv("FLEET_Q_DEAD_POD_THRESHOLD", "60")),
        max_retries=int(os.getenv("FLEET_Q_MAX_RETRIES", "3")),
        backoff_base_delay_ms=int(os.getenv("FLEET_Q_BACKOFF_BASE_DELAY_MS", "50")),
        backoff_max_delay_ms=int(os.getenv("FLEET_Q_BACKOFF_MAX_DELAY_MS", "5000")),
        backoff_max_attempts=int(os.getenv("FLEET_Q_BACKOFF_MAX_ATTEMPTS", "5")),
        local_db_path=os.getenv("FLEET_Q_LOCAL_DB_PATH", "/tmp/fleet_q_local.db"),
    )
