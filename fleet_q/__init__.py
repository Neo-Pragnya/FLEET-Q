"""
FLEET-Q: Federated Leaderless Execution & Elastic Tasking Queue

A brokerless distributed task queue designed for EKS environments where pods
cannot communicate directly with each other. Uses Snowflake as the sole
coordination mechanism with leader-based recovery.

Key Features:
- Brokerless architecture using database coordination
- EKS/Kubernetes optimized with network isolation support
- Elastic scaling with automatic load balancing
- Leader-based recovery for fault tolerance
- Atomic operations with strong consistency
- Comprehensive observability and monitoring

Example:
    >>> import httpx
    >>> client = httpx.AsyncClient(base_url="http://localhost:8000")
    >>> response = await client.post("/submit", json={
    ...     "type": "data_processing",
    ...     "args": {"file": "data.csv"}
    ... })
    >>> step_id = response.json()["step_id"]
"""

__version__ = "0.1.0"
__author__ = "FLEET-Q Team"
__email__ = "team@fleet-q.dev"
__description__ = "Brokerless distributed task queue for EKS environments using Snowflake coordination"
__url__ = "https://github.com/fleet-q/fleet-q"
__license__ = "MIT"

from .config import FleetQConfig
from .models import Step, StepPayload, PodHealth, StepStatus, PodStatus

__all__ = [
    "__version__",
    "__author__", 
    "__email__",
    "__description__",
    "__url__",
    "__license__",
    "FleetQConfig",
    "Step", 
    "StepPayload",
    "PodHealth",
    "StepStatus",
    "PodStatus",
]