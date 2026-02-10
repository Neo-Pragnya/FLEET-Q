"""
FLEET-Q Configuration Management

Loads configuration from environment variables with sensible defaults.
Includes adaptive configuration based on pod CPU/memory resources (cgroup-aware).
"""

import os
from dataclasses import dataclass
from typing import Optional

# Try to import pod resource utilities for adaptive configuration
try:
    from cgroup_aware_resources import (
        recommended_fleet_claim_workers,
        recommended_aiomultiprocess_workers,
        recommended_iohub_flush_threads,
        get_pod_resources
    )
    ADAPTIVE_CONFIG_AVAILABLE = True
except ImportError:
    ADAPTIVE_CONFIG_AVAILABLE = False


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
    
    # Capacity management (None = auto-detect from cgroups)
    max_parallelism: Optional[int] = None
    aiomultiprocess_workers: Optional[int] = None
    iohub_flush_threads: Optional[int] = None
    capacity_threshold: float = 0.8
    
    # Resource detection flags
    enable_adaptive_config: bool = True  # Auto-detect from cgroups if available
    
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
    
    # Control Plane configuration
    enable_control_plane: bool = True
    control_plane_flush_interval: float = 20.0  # Seconds (15-30 recommended)
    control_plane_maintenance_interval: float = 3600.0  # 1 hour
    control_plane_base_path: str = "/tmp/fleetq"  # Base path for pod SQLite databases
    control_plane_max_batch_size: int = 1000
    control_plane_min_writers: int = 1
    control_plane_max_writers: int = 8


def load_config() -> FleetQConfig:
    """
    Load configuration from environment variables with adaptive resource detection.
    
    Adaptive Configuration:
    - If FLEET_Q_MAX_PARALLELISM is not set and cgroups are available,
      automatically detects optimal worker counts based on CPU quota
    - Falls back to sensible defaults (8 workers) if auto-detection unavailable
    - Set FLEET_Q_ENABLE_ADAPTIVE_CONFIG=false to disable auto-detection
    
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
    - FLEET_Q_MAX_PARALLELISM: auto-detect or 8
    - FLEET_Q_AIOMULTIPROCESS_WORKERS: auto-detect or 4
    - FLEET_Q_IOHUB_FLUSH_THREADS: auto-detect or 4
    - FLEET_Q_CAPACITY_THRESHOLD: 0.8
    - FLEET_Q_ENABLE_ADAPTIVE_CONFIG: true (enable auto-detection)
    - FLEET_Q_DEAD_POD_THRESHOLD: 60 (seconds)
    - FLEET_Q_MAX_RETRIES: 3
    - FLEET_Q_LOCAL_DB_PATH: /tmp/fleet_q_local.db
    - FLEET_Q_ENABLE_CONTROL_PLANE: true (enable control plane worker)
    - FLEET_Q_CONTROL_PLANE_FLUSH_INTERVAL: 20.0 (seconds, 15-30 recommended)
    - FLEET_Q_CONTROL_PLANE_MAINTENANCE_INTERVAL: 3600.0 (seconds)
    - FLEET_Q_CONTROL_PLANE_BASE_PATH: /tmp/fleetq
    - FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE: 1000
    - FLEET_Q_CONTROL_PLANE_MIN_WRITERS: 1
    - FLEET_Q_CONTROL_PLANE_MAX_WRITERS: 8
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
    
    # Check if adaptive config is enabled
    enable_adaptive = os.getenv("FLEET_Q_ENABLE_ADAPTIVE_CONFIG", "true").lower() == "true"
    
    # Determine worker counts (auto-detect or use defaults)
    max_parallelism = None
    aiomultiprocess_workers = None
    iohub_flush_threads = None
    
    if enable_adaptive and ADAPTIVE_CONFIG_AVAILABLE:
        # Auto-detect from cgroups
        try:
            resources = get_pod_resources()
            print(f"🔍 Pod Resource Detection:")
            print(f"  CPU Cores: {resources.cpu_cores:.2f}")
            print(f"  Memory: {resources.memory_gb:.2f} GB")
            print(f"  Recommended Fleet Workers: {resources.recommended_fleet_workers}")
            print(f"  Recommended AIOMultiprocess Workers: {resources.recommended_aiomultiprocess}")
            print(f"  Recommended IOHub Flush Threads: {resources.recommended_iohub_flush_threads}")
            
            # Use environment variables if set, otherwise use auto-detected values
            max_parallelism = int(os.getenv("FLEET_Q_MAX_PARALLELISM")) if os.getenv("FLEET_Q_MAX_PARALLELISM") else resources.recommended_fleet_workers
            aiomultiprocess_workers = int(os.getenv("FLEET_Q_AIOMULTIPROCESS_WORKERS")) if os.getenv("FLEET_Q_AIOMULTIPROCESS_WORKERS") else resources.recommended_aiomultiprocess
            iohub_flush_threads = int(os.getenv("FLEET_Q_IOHUB_FLUSH_THREADS")) if os.getenv("FLEET_Q_IOHUB_FLUSH_THREADS") else resources.recommended_iohub_flush_threads
            
        except Exception as e:
            print(f"⚠️  Adaptive config failed, using defaults: {e}")
    
    # Fall back to environment variables or hardcoded defaults
    if max_parallelism is None:
        max_parallelism = int(os.getenv("FLEET_Q_MAX_PARALLELISM", "8"))
    if aiomultiprocess_workers is None:
        aiomultiprocess_workers = int(os.getenv("FLEET_Q_AIOMULTIPROCESS_WORKERS", "4"))
    if iohub_flush_threads is None:
        iohub_flush_threads = int(os.getenv("FLEET_Q_IOHUB_FLUSH_THREADS", "4"))
    
    return FleetQConfig(
        pod_id=pod_id,
        snowflake=snowflake_config,
        pod_health_table=os.getenv("FLEET_Q_POD_HEALTH_TABLE", "POD_HEALTH"),
        step_tracker_table=os.getenv("FLEET_Q_STEP_TRACKER_TABLE", "STEP_TRACKER"),
        heartbeat_interval=int(os.getenv("FLEET_Q_HEARTBEAT_INTERVAL", "10")),
        leader_check_interval=int(os.getenv("FLEET_Q_LEADER_CHECK_INTERVAL", "15")),
        recovery_interval=int(os.getenv("FLEET_Q_RECOVERY_INTERVAL", "30")),
        claim_interval=int(os.getenv("FLEET_Q_CLAIM_INTERVAL", "5")),
        max_parallelism=max_parallelism,
        aiomultiprocess_workers=aiomultiprocess_workers,
        iohub_flush_threads=iohub_flush_threads,
        capacity_threshold=float(os.getenv("FLEET_Q_CAPACITY_THRESHOLD", "0.8")),
        enable_adaptive_config=enable_adaptive,
        dead_pod_threshold_seconds=int(os.getenv("FLEET_Q_DEAD_POD_THRESHOLD", "60")),
        max_retries=int(os.getenv("FLEET_Q_MAX_RETRIES", "3")),
        backoff_base_delay_ms=int(os.getenv("FLEET_Q_BACKOFF_BASE_DELAY_MS", "50")),
        backoff_max_delay_ms=int(os.getenv("FLEET_Q_BACKOFF_MAX_DELAY_MS", "5000")),
        backoff_max_attempts=int(os.getenv("FLEET_Q_BACKOFF_MAX_ATTEMPTS", "5")),
        local_db_path=os.getenv("FLEET_Q_LOCAL_DB_PATH", "/tmp/fleet_q_local.db"),
        enable_control_plane=os.getenv("FLEET_Q_ENABLE_CONTROL_PLANE", "true").lower() == "true",
        control_plane_flush_interval=float(os.getenv("FLEET_Q_CONTROL_PLANE_FLUSH_INTERVAL", "20.0")),
        control_plane_maintenance_interval=float(os.getenv("FLEET_Q_CONTROL_PLANE_MAINTENANCE_INTERVAL", "3600.0")),
        control_plane_base_path=os.getenv("FLEET_Q_CONTROL_PLANE_BASE_PATH", "/tmp/fleetq"),
        control_plane_max_batch_size=int(os.getenv("FLEET_Q_CONTROL_PLANE_MAX_BATCH_SIZE", "1000")),
        control_plane_min_writers=int(os.getenv("FLEET_Q_CONTROL_PLANE_MIN_WRITERS", "1")),
        control_plane_max_writers=int(os.getenv("FLEET_Q_CONTROL_PLANE_MAX_WRITERS", "8")),
    )
