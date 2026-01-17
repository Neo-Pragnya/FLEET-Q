# Production Deployment Guide

This guide covers deploying FLEET-Q in production environments, including Kubernetes, EKS, and other container orchestration platforms.

## Deployment Overview

FLEET-Q is designed as a cloud-native application that runs in containerized environments. Key deployment characteristics:

- **Single Process**: All functionality runs in one container process
- **Stateless**: No persistent local state (except ephemeral SQLite)
- **Database-Dependent**: Requires Snowflake connectivity
- **Horizontally Scalable**: Add more pods for increased capacity

## Kubernetes Deployment

### Basic Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-q
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
        env:
        - name: FLEET_Q_SNOWFLAKE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: fleet-q-secrets
              key: snowflake-account
        - name: FLEET_Q_SNOWFLAKE_USER
          valueFrom:
            secretKeyRef:
              name: fleet-q-secrets
              key: snowflake-user
        - name: FLEET_Q_SNOWFLAKE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: fleet-q-secrets
              key: snowflake-password
        - name: FLEET_Q_SNOWFLAKE_DATABASE
          valueFrom:
            configMapKeyRef:
              name: fleet-q-config
              key: snowflake-database
        - name: FLEET_Q_MAX_PARALLELISM
          valueFrom:
            configMapKeyRef:
              name: fleet-q-config
              key: max-parallelism
        - name: FLEET_Q_LOG_LEVEL
          value: "INFO"
        - name: FLEET_Q_LOG_FORMAT
          value: "json"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 40
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 5
          failureThreshold: 3
        startupProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 6
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      restartPolicy: Always
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fleet-q-config
data:
  snowflake-database: "FLEET_Q_PROD"
  snowflake-schema: "PUBLIC"
  snowflake-warehouse: "COMPUTE_WH"
  max-parallelism: "20"
  capacity-threshold: "0.8"
  heartbeat-interval-seconds: "30"
  claim-interval-seconds: "5"
  recovery-interval-seconds: "300"
  dead-pod-threshold-seconds: "180"
  log-level: "INFO"
  log-format: "json"
```

### Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: fleet-q-secrets
type: Opaque
stringData:
  snowflake-account: "your-account.us-east-1"
  snowflake-user: "fleet_q_prod_user"
  snowflake-password: "secure-password-here"
  snowflake-role: "FLEET_Q_ROLE"
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fleet-q-service
  labels:
    app: fleet-q
spec:
  selector:
    app: fleet-q
  ports:
  - name: http
    port: 80
    targetPort: 8000
    protocol: TCP
  type: ClusterIP
```

### Ingress (Optional)

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: fleet-q-ingress
  annotations:
    kubernetes.io/ingress.class: "nginx"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/rate-limit: "100"
spec:
  tls:
  - hosts:
    - fleet-q.example.com
    secretName: fleet-q-tls
  rules:
  - host: fleet-q.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: fleet-q-service
            port:
              number: 80
```

## EKS-Specific Configuration

### IAM Roles and Service Accounts

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fleet-q-service-account
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/FleetQRole
```

### IAM Policy for Snowflake Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:region:account:secret:fleet-q/*"
      ]
    }
  ]
}
```

### Using AWS Secrets Manager

```yaml
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: fleet-q-secret-store
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
      auth:
        serviceAccount:
          name: fleet-q-service-account
---
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: fleet-q-external-secret
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: fleet-q-secret-store
    kind: SecretStore
  target:
    name: fleet-q-secrets
    creationPolicy: Owner
  data:
  - secretKey: snowflake-account
    remoteRef:
      key: fleet-q/snowflake
      property: account
  - secretKey: snowflake-user
    remoteRef:
      key: fleet-q/snowflake
      property: user
  - secretKey: snowflake-password
    remoteRef:
      key: fleet-q/snowflake
      property: password
```

## Horizontal Pod Autoscaler

Scale FLEET-Q based on queue depth:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: fleet-q-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: fleet-q
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: External
    external:
      metric:
        name: fleet_q_pending_steps
      target:
        type: AverageValue
        averageValue: "10"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
```

## Resource Requirements

### CPU and Memory

| Deployment Size | Pods | CPU Request | CPU Limit | Memory Request | Memory Limit |
|----------------|------|-------------|-----------|----------------|--------------|
| Small | 2-3 | 250m | 500m | 256Mi | 512Mi |
| Medium | 4-6 | 500m | 1000m | 512Mi | 1Gi |
| Large | 8-12 | 1000m | 2000m | 1Gi | 2Gi |

### Storage

FLEET-Q uses minimal storage:
- **Ephemeral SQLite**: ~10MB per pod for recovery operations
- **Logs**: Depends on log retention policy
- **No persistent volumes required**

## Network Configuration

### Security Groups (AWS)

```yaml
# Ingress rules
- port: 8000
  protocol: TCP
  source: ALB Security Group
  description: "HTTP traffic from load balancer"

- port: 443
  protocol: TCP
  source: 0.0.0.0/0
  description: "HTTPS outbound to Snowflake"

# Egress rules
- port: 443
  protocol: TCP
  destination: 0.0.0.0/0
  description: "HTTPS to Snowflake"
```

### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fleet-q-network-policy
spec:
  podSelector:
    matchLabels:
      app: fleet-q
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to: []
    ports:
    - protocol: TCP
      port: 443
    - protocol: TCP
      port: 53
    - protocol: UDP
      port: 53
```

## Monitoring and Observability

### Prometheus Metrics

```yaml
apiVersion: v1
kind: Service
metadata:
  name: fleet-q-metrics
  labels:
    app: fleet-q
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8000"
    prometheus.io/path: "/admin/queue"
spec:
  selector:
    app: fleet-q
  ports:
  - name: metrics
    port: 8000
    targetPort: 8000
```

### ServiceMonitor for Prometheus Operator

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: fleet-q-service-monitor
  labels:
    app: fleet-q
spec:
  selector:
    matchLabels:
      app: fleet-q
  endpoints:
  - port: metrics
    path: /admin/queue
    interval: 30s
```

### Grafana Dashboard

Key metrics to monitor:
- Queue depth (pending steps)
- Processing rate (completed steps per minute)
- Error rate (failed steps percentage)
- Pod capacity utilization
- Leader election stability

## Logging Configuration

### Structured Logging

```yaml
env:
- name: FLEET_Q_LOG_FORMAT
  value: "json"
- name: FLEET_Q_LOG_LEVEL
  value: "INFO"
```

### Log Aggregation with Fluentd

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluentd-config
data:
  fluent.conf: |
    <source>
      @type tail
      path /var/log/containers/fleet-q-*.log
      pos_file /var/log/fluentd-fleet-q.log.pos
      tag kubernetes.fleet-q
      format json
    </source>
    
    <match kubernetes.fleet-q>
      @type elasticsearch
      host elasticsearch.logging.svc.cluster.local
      port 9200
      index_name fleet-q
    </match>
```

## Backup and Disaster Recovery

### Database Backup

FLEET-Q state is stored in Snowflake:
- **POD_HEALTH**: Can be recreated on restart
- **STEP_TRACKER**: Contains critical task data

Backup strategy:
```sql
-- Daily backup of STEP_TRACKER
CREATE TABLE STEP_TRACKER_BACKUP_20240101 AS
SELECT * FROM STEP_TRACKER;

-- Retention policy
DROP TABLE IF EXISTS STEP_TRACKER_BACKUP_20231201;
```

### Disaster Recovery

1. **Database Recovery**: Restore Snowflake from backup
2. **Pod Recovery**: Redeploy pods (stateless)
3. **Leader Election**: Automatic on pod startup
4. **Work Recovery**: Leader detects and requeues orphaned work

## Security Considerations

### Container Security

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  capabilities:
    drop:
    - ALL
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
```

### Pod Security Standards

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: fleet-q
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### Network Security

- Use TLS for all external communication
- Implement network policies to restrict traffic
- Use private subnets for pod deployment
- Encrypt secrets at rest and in transit

## Performance Tuning

### JVM Tuning (if using Java-based tools)

```yaml
env:
- name: JAVA_OPTS
  value: "-Xmx512m -Xms256m -XX:+UseG1GC"
```

### Database Connection Tuning

```yaml
env:
- name: FLEET_Q_SNOWFLAKE_CONNECTION_POOL_SIZE
  value: "10"
- name: FLEET_Q_SNOWFLAKE_CONNECTION_TIMEOUT
  value: "30"
```

### Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: fleet-q-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: fleet-q
```

## Troubleshooting Deployment

### Common Issues

**Pods not starting**
```bash
# Check pod status
kubectl get pods -l app=fleet-q

# Check pod logs
kubectl logs -l app=fleet-q --tail=100

# Check events
kubectl get events --sort-by=.metadata.creationTimestamp
```

**Database connection issues**
```bash
# Test connectivity from pod
kubectl exec -it deployment/fleet-q -- python -c "
from fleet_q.config import load_config
from fleet_q.snowflake_storage import SnowflakeStorage
config = load_config()
storage = SnowflakeStorage(config)
print('Connection successful')
"
```

**Leader election problems**
```bash
# Check leader status
kubectl exec -it deployment/fleet-q -- curl localhost:8000/admin/leader

# Check pod health
kubectl exec -it deployment/fleet-q -- curl localhost:8000/health
```

## Deployment Checklist

### Pre-Deployment

- [ ] Snowflake database and tables created
- [ ] Secrets and ConfigMaps configured
- [ ] Resource limits set appropriately
- [ ] Health checks configured
- [ ] Monitoring and logging set up

### Post-Deployment

- [ ] Verify all pods are running
- [ ] Check leader election is working
- [ ] Test step submission and processing
- [ ] Verify monitoring and alerting
- [ ] Test disaster recovery procedures

### Production Readiness

- [ ] Load testing completed
- [ ] Security review passed
- [ ] Backup and recovery tested
- [ ] Runbooks created
- [ ] Team training completed

## Next Steps

- **[Docker Guide](docker.md)** - Container-specific deployment details
- **[Monitoring](monitoring.md)** - Comprehensive monitoring setup
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions