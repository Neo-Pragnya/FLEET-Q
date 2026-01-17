#!/usr/bin/env python3
"""
Environment Configuration Examples

This example demonstrates how to configure FLEET-Q for different environments
and shows the configuration patterns for development, testing, and production.
"""

import os
import asyncio
import tempfile
from pathlib import Path
from typing import Dict, Any
from fleet_q.config import FleetQConfig


class EnvironmentConfigManager:
    """Manages environment-specific configurations for FLEET-Q"""
    
    @staticmethod
    def development_config() -> Dict[str, str]:
        """Development environment configuration"""
        return {
            # Use SQLite for local development
            "FLEET_Q_STORAGE_MODE": "sqlite",
            "FLEET_Q_SQLITE_DB_PATH": "./fleet_q_dev.db",
            
            # Development-friendly settings
            "FLEET_Q_LOG_LEVEL": "DEBUG",
            "FLEET_Q_LOG_FORMAT": "console",
            "FLEET_Q_MAX_PARALLELISM": "3",
            "FLEET_Q_CAPACITY_THRESHOLD": "0.7",
            
            # Fast intervals for quick feedback
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS": "10",
            "FLEET_Q_CLAIM_INTERVAL_SECONDS": "2",
            "FLEET_Q_RECOVERY_INTERVAL_SECONDS": "30",
            "FLEET_Q_DEAD_POD_THRESHOLD_SECONDS": "20",
            
            # Local API binding
            "FLEET_Q_API_HOST": "127.0.0.1",
            "FLEET_Q_API_PORT": "8000",
            
            # Development pod ID
            "FLEET_Q_POD_ID": f"dev-{os.getenv('USER', 'developer')}",
        }
    
    @staticmethod
    def testing_config() -> Dict[str, str]:
        """Testing environment configuration"""
        return {
            # In-memory SQLite for tests
            "FLEET_Q_STORAGE_MODE": "sqlite",
            "FLEET_Q_SQLITE_DB_PATH": ":memory:",
            
            # Minimal logging for test performance
            "FLEET_Q_LOG_LEVEL": "WARNING",
            "FLEET_Q_LOG_FORMAT": "json",
            "FLEET_Q_MAX_PARALLELISM": "2",
            "FLEET_Q_CAPACITY_THRESHOLD": "0.8",
            
            # Very fast intervals for test speed
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS": "1",
            "FLEET_Q_CLAIM_INTERVAL_SECONDS": "0.5",
            "FLEET_Q_RECOVERY_INTERVAL_SECONDS": "5",
            "FLEET_Q_DEAD_POD_THRESHOLD_SECONDS": "3",
            
            # Random port for parallel testing
            "FLEET_Q_API_HOST": "127.0.0.1",
            "FLEET_Q_API_PORT": "0",  # Random available port
            
            # Test pod ID
            "FLEET_Q_POD_ID": "test-pod-001",
        }
    
    @staticmethod
    def staging_config() -> Dict[str, str]:
        """Staging environment configuration"""
        return {
            # Real Snowflake for staging
            "FLEET_Q_STORAGE_MODE": "snowflake",
            "FLEET_Q_SNOWFLAKE_ACCOUNT": "staging-account.us-east-1",
            "FLEET_Q_SNOWFLAKE_USER": "fleet_q_staging",
            "FLEET_Q_SNOWFLAKE_PASSWORD": "${STAGING_SNOWFLAKE_PASSWORD}",
            "FLEET_Q_SNOWFLAKE_DATABASE": "FLEET_Q_STAGING",
            "FLEET_Q_SNOWFLAKE_SCHEMA": "PUBLIC",
            "FLEET_Q_SNOWFLAKE_WAREHOUSE": "STAGING_WH",
            "FLEET_Q_SNOWFLAKE_ROLE": "FLEET_Q_STAGING_ROLE",
            
            # Production-like settings
            "FLEET_Q_LOG_LEVEL": "INFO",
            "FLEET_Q_LOG_FORMAT": "json",
            "FLEET_Q_MAX_PARALLELISM": "15",
            "FLEET_Q_CAPACITY_THRESHOLD": "0.8",
            
            # Standard intervals
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS": "30",
            "FLEET_Q_CLAIM_INTERVAL_SECONDS": "5",
            "FLEET_Q_RECOVERY_INTERVAL_SECONDS": "300",
            "FLEET_Q_DEAD_POD_THRESHOLD_SECONDS": "180",
            
            # Container-friendly API binding
            "FLEET_Q_API_HOST": "0.0.0.0",
            "FLEET_Q_API_PORT": "8000",
            
            # Backoff configuration
            "FLEET_Q_DEFAULT_MAX_ATTEMPTS": "5",
            "FLEET_Q_DEFAULT_BASE_DELAY_MS": "100",
            "FLEET_Q_DEFAULT_MAX_DELAY_MS": "30000",
        }
    
    @staticmethod
    def production_config() -> Dict[str, str]:
        """Production environment configuration"""
        return {
            # Production Snowflake
            "FLEET_Q_STORAGE_MODE": "snowflake",
            "FLEET_Q_SNOWFLAKE_ACCOUNT": "prod-account.us-east-1",
            "FLEET_Q_SNOWFLAKE_USER": "fleet_q_prod",
            "FLEET_Q_SNOWFLAKE_PASSWORD": "${PROD_SNOWFLAKE_PASSWORD}",
            "FLEET_Q_SNOWFLAKE_DATABASE": "FLEET_Q_PROD",
            "FLEET_Q_SNOWFLAKE_SCHEMA": "PUBLIC",
            "FLEET_Q_SNOWFLAKE_WAREHOUSE": "PROD_WH",
            "FLEET_Q_SNOWFLAKE_ROLE": "FLEET_Q_PROD_ROLE",
            
            # Production logging
            "FLEET_Q_LOG_LEVEL": "INFO",
            "FLEET_Q_LOG_FORMAT": "json",
            
            # High-capacity settings
            "FLEET_Q_MAX_PARALLELISM": "25",
            "FLEET_Q_CAPACITY_THRESHOLD": "0.8",
            
            # Production intervals
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS": "30",
            "FLEET_Q_CLAIM_INTERVAL_SECONDS": "5",
            "FLEET_Q_RECOVERY_INTERVAL_SECONDS": "300",
            "FLEET_Q_DEAD_POD_THRESHOLD_SECONDS": "180",
            "FLEET_Q_LEADER_CHECK_INTERVAL_SECONDS": "60",
            
            # Production API
            "FLEET_Q_API_HOST": "0.0.0.0",
            "FLEET_Q_API_PORT": "8000",
            
            # Conservative backoff for stability
            "FLEET_Q_DEFAULT_MAX_ATTEMPTS": "5",
            "FLEET_Q_DEFAULT_BASE_DELAY_MS": "200",
            "FLEET_Q_DEFAULT_MAX_DELAY_MS": "30000",
        }
    
    @staticmethod
    def high_throughput_config() -> Dict[str, str]:
        """High-throughput production configuration"""
        base_config = EnvironmentConfigManager.production_config()
        
        # Override for high throughput
        base_config.update({
            "FLEET_Q_SNOWFLAKE_WAREHOUSE": "XLARGE_WH",  # Larger warehouse
            "FLEET_Q_MAX_PARALLELISM": "50",
            "FLEET_Q_CAPACITY_THRESHOLD": "0.9",  # Use more capacity
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS": "20",  # More frequent
            "FLEET_Q_CLAIM_INTERVAL_SECONDS": "2",  # More aggressive claiming
            "FLEET_Q_RECOVERY_INTERVAL_SECONDS": "180",  # More frequent recovery
            
            # Aggressive backoff
            "FLEET_Q_DEFAULT_MAX_ATTEMPTS": "3",
            "FLEET_Q_DEFAULT_BASE_DELAY_MS": "50",
            "FLEET_Q_DEFAULT_MAX_DELAY_MS": "10000",
            
            # Reduced logging for performance
            "FLEET_Q_LOG_LEVEL": "WARNING",
        })
        
        return base_config
    
    @staticmethod
    def apply_environment(env_config: Dict[str, str]):
        """Apply environment configuration to current process"""
        for key, value in env_config.items():
            os.environ[key] = value
    
    @staticmethod
    def create_env_file(env_config: Dict[str, str], file_path: str):
        """Create .env file from configuration"""
        with open(file_path, 'w') as f:
            f.write("# FLEET-Q Environment Configuration\n")
            f.write("# Generated automatically - do not edit manually\n\n")
            
            for key, value in env_config.items():
                f.write(f"{key}={value}\n")
    
    @staticmethod
    def validate_config(env_name: str) -> bool:
        """Validate configuration for environment"""
        try:
            config = FleetQConfig()
            print(f"✅ {env_name} configuration is valid")
            print(f"   Storage mode: {config.storage_mode}")
            print(f"   Max parallelism: {config.max_parallelism}")
            print(f"   Log level: {config.log_level}")
            return True
        except Exception as e:
            print(f"❌ {env_name} configuration is invalid: {e}")
            return False


def demonstrate_configurations():
    """Demonstrate different environment configurations"""
    
    print("🔧 FLEET-Q Environment Configuration Examples")
    print("=" * 60)
    
    environments = {
        "Development": EnvironmentConfigManager.development_config(),
        "Testing": EnvironmentConfigManager.testing_config(),
        "Staging": EnvironmentConfigManager.staging_config(),
        "Production": EnvironmentConfigManager.production_config(),
        "High-Throughput": EnvironmentConfigManager.high_throughput_config(),
    }
    
    for env_name, env_config in environments.items():
        print(f"\n📋 {env_name} Configuration:")
        print("-" * 40)
        
        # Show key configuration values
        key_configs = [
            "FLEET_Q_STORAGE_MODE",
            "FLEET_Q_MAX_PARALLELISM", 
            "FLEET_Q_LOG_LEVEL",
            "FLEET_Q_HEARTBEAT_INTERVAL_SECONDS",
            "FLEET_Q_CLAIM_INTERVAL_SECONDS"
        ]
        
        for key in key_configs:
            if key in env_config:
                print(f"  {key}: {env_config[key]}")
        
        # Create example .env file
        env_file = f".env.{env_name.lower()}"
        EnvironmentConfigManager.create_env_file(env_config, env_file)
        print(f"  📄 Created: {env_file}")
        
        # Validate configuration (for non-Snowflake configs)
        if env_config.get("FLEET_Q_STORAGE_MODE") == "sqlite":
            # Apply config temporarily for validation
            original_env = dict(os.environ)
            try:
                EnvironmentConfigManager.apply_environment(env_config)
                EnvironmentConfigManager.validate_config(env_name)
            finally:
                # Restore original environment
                os.environ.clear()
                os.environ.update(original_env)


def create_docker_compose_examples():
    """Create docker-compose examples for different environments"""
    
    print("\n🐳 Creating Docker Compose Examples")
    print("-" * 40)
    
    # Development docker-compose
    dev_compose = """version: '3.8'
services:
  fleet-q-dev:
    image: fleet-q:latest
    ports:
      - "8000:8000"
    environment:
      - FLEET_Q_STORAGE_MODE=sqlite
      - FLEET_Q_SQLITE_DB_PATH=/app/data/fleet_q_dev.db
      - FLEET_Q_LOG_LEVEL=DEBUG
      - FLEET_Q_LOG_FORMAT=console
      - FLEET_Q_MAX_PARALLELISM=3
      - FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=10
      - FLEET_Q_CLAIM_INTERVAL_SECONDS=2
      - FLEET_Q_POD_ID=dev-single-pod
    volumes:
      - fleet-q-dev-data:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  fleet-q-dev-data:
"""
    
    with open("docker-compose.dev.yml", "w") as f:
        f.write(dev_compose)
    print("📄 Created: docker-compose.dev.yml")
    
    # Multi-pod development
    multi_pod_compose = """version: '3.8'
services:
  fleet-q-leader:
    image: fleet-q:latest
    ports:
      - "8000:8000"
    environment:
      - FLEET_Q_STORAGE_MODE=sqlite
      - FLEET_Q_SQLITE_DB_PATH=/app/data/fleet_q_shared.db
      - FLEET_Q_LOG_LEVEL=DEBUG
      - FLEET_Q_LOG_FORMAT=console
      - FLEET_Q_MAX_PARALLELISM=5
      - FLEET_Q_POD_ID=leader-pod
    volumes:
      - fleet-q-shared-data:/app/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  fleet-q-worker-1:
    image: fleet-q:latest
    ports:
      - "8001:8000"
    environment:
      - FLEET_Q_STORAGE_MODE=sqlite
      - FLEET_Q_SQLITE_DB_PATH=/app/data/fleet_q_shared.db
      - FLEET_Q_LOG_LEVEL=DEBUG
      - FLEET_Q_LOG_FORMAT=console
      - FLEET_Q_MAX_PARALLELISM=3
      - FLEET_Q_POD_ID=worker-pod-1
    volumes:
      - fleet-q-shared-data:/app/data
    depends_on:
      - fleet-q-leader

  fleet-q-worker-2:
    image: fleet-q:latest
    ports:
      - "8002:8000"
    environment:
      - FLEET_Q_STORAGE_MODE=sqlite
      - FLEET_Q_SQLITE_DB_PATH=/app/data/fleet_q_shared.db
      - FLEET_Q_LOG_LEVEL=DEBUG
      - FLEET_Q_LOG_FORMAT=console
      - FLEET_Q_MAX_PARALLELISM=3
      - FLEET_Q_POD_ID=worker-pod-2
    volumes:
      - fleet-q-shared-data:/app/data
    depends_on:
      - fleet-q-leader

volumes:
  fleet-q-shared-data:
"""
    
    with open("docker-compose.multi-pod.yml", "w") as f:
        f.write(multi_pod_compose)
    print("📄 Created: docker-compose.multi-pod.yml")


def create_kubernetes_examples():
    """Create Kubernetes deployment examples"""
    
    print("\n☸️  Creating Kubernetes Examples")
    print("-" * 40)
    
    # ConfigMap
    configmap = """apiVersion: v1
kind: ConfigMap
metadata:
  name: fleet-q-config
  namespace: fleet-q
data:
  FLEET_Q_STORAGE_MODE: "snowflake"
  FLEET_Q_SNOWFLAKE_DATABASE: "FLEET_Q_PROD"
  FLEET_Q_SNOWFLAKE_SCHEMA: "PUBLIC"
  FLEET_Q_SNOWFLAKE_WAREHOUSE: "PROD_WH"
  FLEET_Q_SNOWFLAKE_ROLE: "FLEET_Q_PROD_ROLE"
  FLEET_Q_LOG_LEVEL: "INFO"
  FLEET_Q_LOG_FORMAT: "json"
  FLEET_Q_MAX_PARALLELISM: "20"
  FLEET_Q_CAPACITY_THRESHOLD: "0.8"
  FLEET_Q_HEARTBEAT_INTERVAL_SECONDS: "30"
  FLEET_Q_CLAIM_INTERVAL_SECONDS: "5"
  FLEET_Q_RECOVERY_INTERVAL_SECONDS: "300"
  FLEET_Q_API_HOST: "0.0.0.0"
  FLEET_Q_API_PORT: "8000"
"""
    
    with open("k8s-configmap.yaml", "w") as f:
        f.write(configmap)
    print("📄 Created: k8s-configmap.yaml")
    
    # Secret
    secret = """apiVersion: v1
kind: Secret
metadata:
  name: fleet-q-secrets
  namespace: fleet-q
type: Opaque
stringData:
  FLEET_Q_SNOWFLAKE_ACCOUNT: "prod-account.us-east-1"
  FLEET_Q_SNOWFLAKE_USER: "fleet_q_prod"
  FLEET_Q_SNOWFLAKE_PASSWORD: "secure-password-here"
"""
    
    with open("k8s-secret.yaml", "w") as f:
        f.write(secret)
    print("📄 Created: k8s-secret.yaml")
    
    # Deployment
    deployment = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-q
  namespace: fleet-q
  labels:
    app: fleet-q
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fleet-q
  template:
    metadata:
      labels:
        app: fleet-q
    spec:
      containers:
      - name: fleet-q
        image: fleet-q:1.0.0
        ports:
        - containerPort: 8000
          name: http
        envFrom:
        - configMapRef:
            name: fleet-q-config
        - secretRef:
            name: fleet-q-secrets
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 40
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
"""
    
    with open("k8s-deployment.yaml", "w") as f:
        f.write(deployment)
    print("📄 Created: k8s-deployment.yaml")


def main():
    """Main example function"""
    demonstrate_configurations()
    create_docker_compose_examples()
    create_kubernetes_examples()
    
    print("\n✅ Environment configuration examples created!")
    print("\nUsage:")
    print("  Development: source .env.development && fleet-q")
    print("  Testing:     source .env.testing && python -m pytest")
    print("  Docker Dev:  docker-compose -f docker-compose.dev.yml up")
    print("  Multi-pod:   docker-compose -f docker-compose.multi-pod.yml up")
    print("  Kubernetes:  kubectl apply -f k8s-*.yaml")


if __name__ == "__main__":
    main()