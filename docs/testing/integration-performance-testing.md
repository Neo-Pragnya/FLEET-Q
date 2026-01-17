# Integration and Performance Testing Guide

This guide covers the comprehensive integration and performance testing suite for FLEET-Q, including multi-pod simulation, end-to-end workflow validation, failure injection testing, and performance benchmarking.

## Overview

The FLEET-Q testing suite provides three main categories of tests:

1. **Integration Tests** - Multi-pod simulation and end-to-end workflow validation
2. **Performance Tests** - Load testing and resource usage optimization
3. **Benchmarking** - Standardized performance measurements

## Test Structure

### Integration Test Suite (`tests/test_integration_suite.py`)

#### Multi-Pod Integration Tests
- **Leader Election Testing**: Validates deterministic leader election with multiple pods
- **Concurrent Step Claiming**: Tests atomic claiming across multiple pods
- **Pod Failure and Recovery**: Simulates pod failures and validates recovery mechanisms
- **Concurrent Processing**: Tests system behavior under high concurrent load

#### End-to-End Workflow Tests
- **Complete Step Lifecycle**: Validates full step lifecycle from submission to completion
- **Step Retry Workflow**: Tests retry mechanisms with failure and recovery
- **Idempotency Workflow**: Validates idempotency key generation and usage

#### Failure Injection Tests
- **Cascading Pod Failures**: Tests system resilience under multiple pod failures
- **Storage Error Recovery**: Validates recovery from storage errors
- **Leader Election Under Stress**: Tests leader election stability during failures
- **Recovery DLQ Operations**: Validates Dead Letter Queue operations during recovery

### Performance Test Suite (`tests/test_performance_load.py`)

#### High Concurrency Load Tests
- **High Volume Step Submission**: Tests submission performance with large batches
- **Concurrent Claiming Performance**: Measures claiming throughput and latency
- **Mixed Workload Performance**: Tests performance under mixed operations

#### Recovery Performance Tests
- **Large Scale Recovery**: Tests recovery performance with many orphaned steps
- **Recovery Under Load**: Validates recovery while system is under continuous load

#### Resource Usage Tests
- **Memory Usage Optimization**: Monitors memory usage under various loads
- **CPU Usage Efficiency**: Measures CPU efficiency during processing

## Running Tests

### Quick Start

Run all integration tests:
```bash
python scripts/run_integration_tests.py --suite all --verbose
```

Run specific test suites:
```bash
# Integration tests only
python scripts/run_integration_tests.py --suite integration

# Performance tests only
python scripts/run_integration_tests.py --suite performance
```

### Advanced Usage

Generate detailed reports:
```bash
python scripts/run_integration_tests.py --suite all --report --verbose
```

Custom timeout settings:
```bash
python scripts/run_integration_tests.py --suite all --timeout 600
```

### Direct pytest Execution

Run specific test classes:
```bash
# Multi-pod integration tests
pytest tests/test_integration_suite.py::TestMultiPodIntegration -v

# Performance tests
pytest tests/test_performance_load.py::TestHighConcurrencyLoad -v

# Specific test method
pytest tests/test_integration_suite.py::TestMultiPodIntegration::test_multi_pod_leader_election -v
```

## Performance Benchmarking

### Benchmark Script

The `scripts/benchmark_performance.py` script provides standardized performance benchmarks:

```bash
# Run all benchmarks
python scripts/benchmark_performance.py --benchmark all --pods 5 --steps 1000

# Specific benchmarks
python scripts/benchmark_performance.py --benchmark claiming --steps 2000
python scripts/benchmark_performance.py --benchmark recovery --pods 6
python scripts/benchmark_performance.py --benchmark resource --duration 120
```

### Benchmark Types

#### Step Claiming Benchmark
- Measures step submission and claiming throughput
- Tests atomic claiming performance
- Monitors memory usage during claiming

#### Recovery Performance Benchmark
- Tests recovery time with orphaned steps
- Measures recovery efficiency
- Validates system stability after recovery

#### Resource Usage Benchmark
- Monitors CPU and memory usage under sustained load
- Tests resource optimization
- Measures system efficiency

#### Concurrent Processing Benchmark
- Tests performance under high concurrent load
- Measures concurrent processing throughput
- Validates system scalability

### Benchmark Results

Benchmarks generate detailed JSON reports with:
- Performance metrics (throughput, latency, resource usage)
- Configuration details
- System information
- Success/failure status

Example benchmark output:
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "configuration": {
    "num_pods": 5,
    "system_info": {
      "cpu_count": 8,
      "memory_total_gb": 16.0
    }
  },
  "benchmarks": [
    {
      "benchmark_name": "step_claiming",
      "metrics": {
        "claiming_throughput_ops_per_sec": 45.2,
        "average_latency_ms": 22.1,
        "memory_usage_mb": 125.3
      },
      "success": true
    }
  ]
}
```

## Resource Monitoring

### Real-Time Monitoring

The `scripts/monitor_resources.py` script provides real-time resource monitoring:

```bash
# Basic monitoring
python scripts/monitor_resources.py --duration 300 --interval 5

# Monitor with queue statistics
python scripts/monitor_resources.py --storage /path/to/fleet_q.db --verbose

# Generate performance plots
python scripts/monitor_resources.py --plot --output monitoring_data.csv
```

### Monitoring Features

- **CPU Usage**: Real-time CPU utilization tracking
- **Memory Usage**: Process and system memory monitoring
- **Queue Statistics**: Step counts and processing rates
- **Alert System**: Configurable thresholds for resource alerts
- **Data Export**: CSV export for analysis
- **Visualization**: Performance plots (requires matplotlib)

### Alert Thresholds

Configure custom alert thresholds:
```bash
python scripts/monitor_resources.py --alert-cpu 70 --alert-memory 800
```

Default alerts:
- CPU usage > 80%
- Memory usage > 1000MB
- System memory > 90%
- Disk usage > 90%
- Queue backlog > 1000 steps
- Failure rate > 10%

## Test Configuration

### Environment Variables

Set test-specific environment variables:
```bash
export FLEET_Q_LOG_LEVEL=DEBUG
export FLEET_Q_LOG_FORMAT=console
export FLEET_Q_MAX_PARALLELISM=10
export FLEET_Q_CAPACITY_THRESHOLD=0.8
```

### Pytest Configuration

Use the integration-specific pytest configuration:
```bash
pytest -c pytest.integration.ini tests/test_integration_suite.py
```

Configuration includes:
- Async test support
- Timeout settings (300s default)
- Structured logging
- Test markers for categorization

### Test Markers

Tests are categorized with markers:
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.performance` - Performance tests
- `@pytest.mark.slow` - Tests taking >30 seconds
- `@pytest.mark.requires_resources` - Resource-intensive tests

Run specific categories:
```bash
pytest -m integration
pytest -m "performance and not slow"
```

## Performance Expectations

### Minimum Performance Requirements

Based on requirements validation:

#### Step Claiming (Requirement 2.1, 3.1)
- **Throughput**: ≥10 steps/second submission
- **Claiming**: ≥8 steps/second claiming
- **Latency**: ≤5000ms average
- **Memory**: ≤500MB for 500 steps

#### Recovery Performance (Requirement 6.1)
- **Recovery Time**: ≤40 seconds for 500 orphaned steps
- **Efficiency**: ≥95% step preservation
- **Memory**: ≤200MB growth during recovery

#### Concurrent Processing (Requirement 2.1, 3.1, 6.1)
- **Throughput**: ≥4 steps/second under load
- **Success Rate**: ≥95% step processing
- **Resource Usage**: ≤80% CPU, ≤700MB memory

### Performance Optimization Tips

1. **Claiming Intervals**: Reduce `claim_interval_seconds` for higher throughput
2. **Capacity Threshold**: Increase `capacity_threshold` for more aggressive claiming
3. **Parallelism**: Tune `max_parallelism` based on system resources
4. **Heartbeat Frequency**: Balance between responsiveness and overhead

## Troubleshooting

### Common Issues

#### Test Timeouts
- Increase timeout with `--timeout` parameter
- Check system resources during test execution
- Reduce test load (fewer steps/pods) for slower systems

#### Memory Issues
- Monitor memory usage with resource monitoring
- Reduce `max_parallelism` if memory constrained
- Check for memory leaks in long-running tests

#### Storage Errors
- Ensure SQLite database is writable
- Check disk space availability
- Verify no other processes are using the database

#### Pod Communication Issues
- Check heartbeat intervals are appropriate
- Verify dead pod threshold settings
- Monitor leader election stability

### Debug Mode

Enable debug logging for detailed test output:
```bash
export FLEET_Q_LOG_LEVEL=DEBUG
python scripts/run_integration_tests.py --suite integration --verbose
```

### Performance Analysis

Analyze performance issues:
1. Run resource monitoring during tests
2. Generate performance plots
3. Compare benchmark results over time
4. Profile specific operations

## Continuous Integration

### CI/CD Integration

Example GitHub Actions workflow:
```yaml
name: Integration Tests
on: [push, pull_request]
jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -e .[test]
      - name: Run integration tests
        run: python scripts/run_integration_tests.py --suite all --timeout 600
      - name: Upload test results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: integration_test_report.json
```

### Performance Regression Testing

Monitor performance over time:
1. Run benchmarks on each commit
2. Compare results to baseline
3. Alert on performance regressions
4. Track performance trends

## Best Practices

### Test Development

1. **Isolation**: Each test should be independent
2. **Cleanup**: Always clean up resources after tests
3. **Timeouts**: Set appropriate timeouts for async operations
4. **Assertions**: Use descriptive assertion messages
5. **Logging**: Include relevant context in test logs

### Performance Testing

1. **Baseline**: Establish performance baselines
2. **Consistency**: Run tests in consistent environments
3. **Monitoring**: Monitor system resources during tests
4. **Analysis**: Analyze results for trends and regressions
5. **Documentation**: Document performance expectations

### Resource Management

1. **Limits**: Set resource limits for test environments
2. **Cleanup**: Clean up temporary files and databases
3. **Monitoring**: Monitor resource usage during tests
4. **Optimization**: Optimize test efficiency
5. **Scaling**: Scale tests appropriately for CI environments

## Conclusion

The FLEET-Q integration and performance testing suite provides comprehensive validation of system functionality, performance, and reliability. Regular execution of these tests ensures system quality and helps identify performance regressions early in the development cycle.

For questions or issues with the testing suite, refer to the troubleshooting section or consult the development team.