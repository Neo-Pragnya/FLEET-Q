# FLEET-Q Test Data Strategy

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Status:** Active  
**Owner:** FLEET-Q Testing Team

---

## Table of Contents

1. [Overview](#overview)
2. [Data Categories](#data-categories)
3. [Synthetic Data Generation](#synthetic-data-generation)
4. [Production Data Handling](#production-data-handling)
5. [Edge Case Catalog](#edge-case-catalog)
6. [Chaos Data](#chaos-data)
7. [Data Refresh Strategy](#data-refresh-strategy)
8. [Security & Privacy](#security--privacy)
9. [Data Management](#data-management)

---

## Overview

### Purpose

This document defines the **test data strategy** for FLEET-Q, ensuring tests have access to realistic, diverse, and compliant data across all test categories.

### Principles

1. **Synthetic-First:** Generate synthetic data whenever possible
2. **Privacy-Preserving:** Never use real PII in tests
3. **Deterministic:** Tests must be reproducible
4. **Comprehensive:** Cover normal, edge, and chaos cases
5. **Isolated:** Each test run uses isolated data
6. **Realistic:** Data resembles production patterns

### Data Requirements by Test Type

| Test Type | Data Needs | Volume | Diversity | Realism |
|-----------|-----------|---------|-----------|---------|
| **BDD** | Full workflows | Medium (100s) | High | High |
| **State Machine** | State transitions | Low (10s) | High | Low |
| **Data Invariants** | Query verification | High (1000s) | Medium | Medium |
| **Contracts** | API payloads | Low (10s) | High | High |
| **Resilience** | Failure scenarios | Medium (100s) | Low | Medium |

---

## Data Categories

### 1. Normal/Happy Path Data

**Purpose:** Validate standard operations

**Characteristics:**
- Valid inputs within expected ranges
- Common use cases
- Representative of 80% of production traffic

**Examples:**

```python
# Task submission - normal
{
    "payload": {
        "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
        "inference_config": {
            "temperature": 0.7,
            "max_tokens": 1024
        },
        "messages": [
            {"role": "user", "content": "Explain quantum computing"}
        ]
    },
    "priority": 1,
    "timeout": 300,
    "retry_policy": {
        "max_attempts": 3,
        "backoff_multiplier": 2.0
    }
}

# Expected characteristics
- Payload size: 500-2000 bytes
- Temperature: 0.0-1.0
- Max tokens: 100-4096
- Priority: 0-2
- Timeout: 60-600 seconds
```

### 2. Edge Case Data

**Purpose:** Test boundary conditions

**Characteristics:**
- Minimum/maximum values
- Empty/null fields
- Special characters
- Unusual but valid combinations

**Examples:**

```python
# Edge cases
edge_cases = [
    # Minimum values
    {
        "payload": {"messages": [{"role": "user", "content": "Hi"}]},
        "priority": 0,
        "timeout": 1,
        "retry_policy": {"max_attempts": 1}
    },
    
    # Maximum values
    {
        "payload": {
            "messages": [{"role": "user", "content": "A" * 100000}]  # 100KB
        },
        "priority": 10,
        "timeout": 3600,
        "retry_policy": {"max_attempts": 10}
    },
    
    # Special characters
    {
        "payload": {
            "messages": [{
                "role": "user",
                "content": "Test: \n\t\r\x00 unicode: 你好 emoji: 😀"
            }]
        }
    },
    
    # Empty optional fields
    {
        "payload": {"messages": [{"role": "user", "content": "Test"}]},
        # No priority, timeout, or retry_policy
    }
]
```

### 3. Invalid/Error Data

**Purpose:** Validate error handling

**Characteristics:**
- Schema violations
- Invalid types
- Out-of-range values
- Missing required fields

**Examples:**

```python
invalid_cases = [
    # Missing required field
    {
        "payload": {}  # Missing 'messages'
    },
    
    # Invalid type
    {
        "payload": {
            "messages": "not an array"
        }
    },
    
    # Out of range
    {
        "payload": {"messages": [{"role": "user", "content": "Test"}]},
        "priority": -1  # Negative priority
    },
    
    # Invalid enum
    {
        "payload": {
            "messages": [{"role": "invalid_role", "content": "Test"}]
        }
    }
]
```

### 4. Performance/Load Data

**Purpose:** Stress test system capacity

**Characteristics:**
- High volume
- Concurrent submissions
- Varying payload sizes

**Generation:**

```python
def generate_load_test_data(num_tasks: int = 1000) -> list[dict]:
    """Generate load test data."""
    
    tasks = []
    for i in range(num_tasks):
        task = {
            "task_id": f"load_test_{i}",
            "payload": {
                "model_id": random.choice([
                    "anthropic.claude-3-sonnet-20240229-v1:0",
                    "anthropic.claude-3-haiku-20240307-v1:0"
                ]),
                "messages": [{
                    "role": "user",
                    "content": generate_random_prompt(
                        min_length=100,
                        max_length=5000
                    )
                }],
                "inference_config": {
                    "temperature": random.uniform(0.0, 1.0),
                    "max_tokens": random.randint(100, 4096)
                }
            },
            "priority": random.randint(0, 2),
            "timeout": random.randint(60, 600)
        }
        tasks.append(task)
    
    return tasks
```

### 5. Chaos/Failure Data

**Purpose:** Test resilience and recovery

**Characteristics:**
- Data that triggers specific failure modes
- Simulates external system errors
- Forces retry/recovery logic

**Examples:**

```python
chaos_cases = [
    # Trigger throttling (429 from Bedrock)
    {
        "payload": {
            "model_id": "throttled_model",  # Mock model that returns 429
            "messages": [{"role": "user", "content": "Test"}]
        }
    },
    
    # Trigger timeout
    {
        "payload": {
            "model_id": "slow_model",  # Takes > timeout to respond
            "messages": [{"role": "user", "content": "Test"}]
        },
        "timeout": 1  # Very short timeout
    },
    
    # Trigger validation error
    {
        "payload": {
            "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
            "messages": [{"role": "user", "content": "Test"}],
            "inference_config": {
                "temperature": 5.0  # Out of range for Bedrock
            }
        }
    }
]
```

---

## Synthetic Data Generation

### Data Generator Framework

```python
# data_generator.py
from dataclasses import dataclass
from typing import Optional, Callable
import random
import string
import json
from faker import Faker

fake = Faker()

@dataclass
class DataTemplate:
    """Template for generating test data."""
    name: str
    generator: Callable
    count: int = 1
    seed: Optional[int] = None

class SyntheticDataGenerator:
    """Generate synthetic test data."""
    
    def __init__(self, seed: Optional[int] = 42):
        self.seed = seed
        random.seed(seed)
        fake.seed_instance(seed)
    
    def generate_task_payload(
        self,
        prompt_length: tuple[int, int] = (50, 500),
        temperature_range: tuple[float, float] = (0.0, 1.0),
        max_tokens_range: tuple[int, int] = (100, 4096)
    ) -> dict:
        """Generate realistic task payload."""
        
        return {
            "model_id": random.choice([
                "anthropic.claude-3-sonnet-20240229-v1:0",
                "anthropic.claude-3-haiku-20240307-v1:0",
                "anthropic.claude-3-opus-20240229-v1:0"
            ]),
            "inference_config": {
                "temperature": round(random.uniform(*temperature_range), 2),
                "max_tokens": random.randint(*max_tokens_range),
                "top_p": round(random.uniform(0.1, 1.0), 2)
            },
            "messages": [
                {
                    "role": "user",
                    "content": self.generate_prompt(
                        min_length=prompt_length[0],
                        max_length=prompt_length[1]
                    )
                }
            ]
        }
    
    def generate_prompt(self, min_length: int, max_length: int) -> str:
        """Generate realistic prompt."""
        
        templates = [
            "Explain {topic} in simple terms.",
            "Write a {content_type} about {topic}.",
            "Analyze {topic} from {perspective} perspective.",
            "Compare and contrast {topic1} and {topic2}.",
            "Provide a step-by-step guide for {task}."
        ]
        
        template = random.choice(templates)
        prompt = template.format(
            topic=fake.catch_phrase(),
            content_type=random.choice(["story", "essay", "poem", "report"]),
            perspective=random.choice(["technical", "business", "academic"]),
            topic1=fake.word(),
            topic2=fake.word(),
            task=fake.bs()
        )
        
        # Pad to desired length
        while len(prompt) < min_length:
            prompt += " " + fake.sentence()
        
        return prompt[:max_length]
    
    def generate_task(
        self,
        task_id: Optional[str] = None,
        priority_range: tuple[int, int] = (0, 2),
        timeout_range: tuple[int, int] = (60, 600)
    ) -> dict:
        """Generate complete task."""
        
        return {
            "task_id": task_id or f"task_{fake.uuid4()}",
            "payload": self.generate_task_payload(),
            "priority": random.randint(*priority_range),
            "timeout": random.randint(*timeout_range),
            "retry_policy": {
                "max_attempts": random.randint(1, 5),
                "backoff_multiplier": round(random.uniform(1.5, 3.0), 1),
                "initial_delay": random.randint(1, 10)
            },
            "metadata": {
                "submitted_by": fake.user_name(),
                "request_id": fake.uuid4(),
                "tags": [fake.word() for _ in range(random.randint(0, 3))]
            }
        }
    
    def generate_dataset(
        self,
        num_tasks: int,
        distribution: dict[str, float] = None
    ) -> list[dict]:
        """Generate dataset with specified distribution."""
        
        if distribution is None:
            distribution = {
                'normal': 0.7,
                'edge': 0.2,
                'invalid': 0.1
            }
        
        tasks = []
        
        # Normal cases
        num_normal = int(num_tasks * distribution['normal'])
        for _ in range(num_normal):
            tasks.append(self.generate_task())
        
        # Edge cases
        num_edge = int(num_tasks * distribution['edge'])
        for _ in range(num_edge):
            tasks.append(self.generate_edge_case())
        
        # Invalid cases
        num_invalid = int(num_tasks * distribution['invalid'])
        for _ in range(num_invalid):
            tasks.append(self.generate_invalid_case())
        
        # Shuffle
        random.shuffle(tasks)
        
        return tasks
    
    def generate_edge_case(self) -> dict:
        """Generate edge case task."""
        
        edge_type = random.choice([
            'min_values',
            'max_values',
            'special_chars',
            'empty_optionals'
        ])
        
        if edge_type == 'min_values':
            return {
                "task_id": f"edge_min_{fake.uuid4()}",
                "payload": {
                    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
                    "messages": [{"role": "user", "content": "Hi"}]
                },
                "priority": 0,
                "timeout": 1,
                "retry_policy": {"max_attempts": 1}
            }
        
        elif edge_type == 'max_values':
            return {
                "task_id": f"edge_max_{fake.uuid4()}",
                "payload": {
                    "model_id": "anthropic.claude-3-opus-20240229-v1:0",
                    "messages": [{
                        "role": "user",
                        "content": "A" * 100000  # 100KB
                    }],
                    "inference_config": {
                        "max_tokens": 4096,
                        "temperature": 1.0
                    }
                },
                "priority": 10,
                "timeout": 3600,
                "retry_policy": {"max_attempts": 10}
            }
        
        elif edge_type == 'special_chars':
            return {
                "task_id": f"edge_special_{fake.uuid4()}",
                "payload": {
                    "messages": [{
                        "role": "user",
                        "content": "Test: \n\t\r unicode: 你好 emoji: 😀 🎉"
                    }]
                }
            }
        
        else:  # empty_optionals
            return {
                "task_id": f"edge_empty_{fake.uuid4()}",
                "payload": {
                    "messages": [{"role": "user", "content": "Test"}]
                }
                # No priority, timeout, retry_policy
            }
    
    def generate_invalid_case(self) -> dict:
        """Generate invalid task (for validation testing)."""
        
        invalid_type = random.choice([
            'missing_required',
            'invalid_type',
            'out_of_range',
            'invalid_enum'
        ])
        
        if invalid_type == 'missing_required':
            return {
                "task_id": f"invalid_missing_{fake.uuid4()}",
                "payload": {}  # Missing messages
            }
        
        elif invalid_type == 'invalid_type':
            return {
                "task_id": f"invalid_type_{fake.uuid4()}",
                "payload": {
                    "messages": "not an array"
                }
            }
        
        elif invalid_type == 'out_of_range':
            return {
                "task_id": f"invalid_range_{fake.uuid4()}",
                "payload": {
                    "messages": [{"role": "user", "content": "Test"}]
                },
                "priority": -1  # Negative
            }
        
        else:  # invalid_enum
            return {
                "task_id": f"invalid_enum_{fake.uuid4()}",
                "payload": {
                    "messages": [{
                        "role": "invalid_role",
                        "content": "Test"
                    }]
                }
            }

# Usage
generator = SyntheticDataGenerator(seed=42)

# Generate single task
task = generator.generate_task()

# Generate dataset
dataset = generator.generate_dataset(
    num_tasks=1000,
    distribution={'normal': 0.7, 'edge': 0.2, 'invalid': 0.1}
)

# Save to file
with open('test_data/dataset_1000.json', 'w') as f:
    json.dump(dataset, f, indent=2)
```

### Pre-generated Datasets

Store commonly used datasets for consistent testing:

```
test_data/
├── smoke_test_10.json          # 10 tasks for quick smoke tests
├── regression_100.json         # 100 tasks for regression
├── load_test_1000.json         # 1000 tasks for load testing
├── edge_cases_50.json          # 50 edge cases
├── invalid_cases_20.json       # 20 invalid cases
└── chaos_scenarios_30.json     # 30 chaos scenarios
```

**Generation Script:**

```python
# generate_test_datasets.py
from data_generator import SyntheticDataGenerator

def main():
    generator = SyntheticDataGenerator(seed=42)
    
    datasets = {
        'smoke_test_10.json': generator.generate_dataset(10, {'normal': 1.0}),
        'regression_100.json': generator.generate_dataset(100),
        'load_test_1000.json': generator.generate_dataset(1000),
        'edge_cases_50.json': [generator.generate_edge_case() for _ in range(50)],
        'invalid_cases_20.json': [generator.generate_invalid_case() for _ in range(20)],
    }
    
    for filename, data in datasets.items():
        with open(f'test_data/{filename}', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Generated {filename}: {len(data)} tasks")

if __name__ == '__main__':
    main()
```

---

## Production Data Handling

### Anonymization Strategy

**NEVER use real production data directly in tests.**

If production data is needed for realism:

1. **Extract Schema Only:**
   ```python
   # Extract schema, not actual data
   schema = {
       "payload": {
           "model_id": "<string>",
           "messages": [{"role": "<string>", "content": "<string>"}]
       },
       "priority": "<int>",
       "timeout": "<int>"
   }
   ```

2. **Anonymize PII:**
   ```python
   from faker import Faker
   
   def anonymize_task(task: dict) -> dict:
       """Anonymize PII in task."""
       fake = Faker()
       
       # Replace user content with synthetic
       for msg in task['payload']['messages']:
           msg['content'] = fake.text(max_nb_chars=len(msg['content']))
       
       # Replace metadata
       if 'metadata' in task:
           task['metadata']['submitted_by'] = fake.user_name()
           task['metadata']['request_id'] = fake.uuid4()
       
       return task
   ```

3. **Statistical Sampling:**
   ```python
   # Extract statistical properties, generate synthetic data
   stats = {
       "prompt_length_mean": 452,
       "prompt_length_std": 189,
       "temperature_mean": 0.72,
       "temperature_std": 0.15,
       "priority_distribution": {0: 0.3, 1: 0.5, 2: 0.2}
   }
   
   # Use stats to generate realistic synthetic data
   def generate_from_stats(stats: dict) -> dict:
       prompt_length = int(random.gauss(
           stats['prompt_length_mean'],
           stats['prompt_length_std']
       ))
       # ... generate task matching distribution
   ```

### Production Data Access Policy

| Data Type | Access Level | Anonymization | Usage |
|-----------|--------------|---------------|-------|
| **Task Payloads** | ❌ Prohibited | N/A | Use synthetic only |
| **User Metadata** | ❌ Prohibited | N/A | Use synthetic only |
| **System Metrics** | ✅ Allowed | Not required | Performance baselines |
| **Error Logs** | ✅ Allowed | Required | Error pattern analysis |
| **Schema Definitions** | ✅ Allowed | Not required | Contract validation |

---

## Edge Case Catalog

### Comprehensive Edge Cases

```python
# edge_case_catalog.py
EDGE_CASES = {
    "boundary_values": [
        {
            "name": "min_prompt_length",
            "payload": {"messages": [{"role": "user", "content": ""}]},
            "expected": "ValidationError"
        },
        {
            "name": "max_prompt_length",
            "payload": {"messages": [{"role": "user", "content": "A" * 200000}]},
            "expected": "Success or ValidationError (depending on limit)"
        },
        {
            "name": "zero_timeout",
            "timeout": 0,
            "expected": "ValidationError"
        },
        {
            "name": "max_timeout",
            "timeout": 86400,  # 24 hours
            "expected": "Success"
        }
    ],
    
    "special_characters": [
        {
            "name": "unicode",
            "content": "Hello 世界 مرحبا мир שלום",
            "expected": "Success"
        },
        {
            "name": "emojis",
            "content": "Test 😀 🎉 🚀 ❤️ 👍",
            "expected": "Success"
        },
        {
            "name": "control_chars",
            "content": "Test\n\t\r\x00\x1b",
            "expected": "Success (sanitized)"
        },
        {
            "name": "sql_injection",
            "content": "'; DROP TABLE tasks; --",
            "expected": "Success (no SQL injection)"
        },
        {
            "name": "json_special",
            "content": 'Test " \\ / \b \f \n \r \t',
            "expected": "Success (escaped)"
        }
    ],
    
    "null_and_empty": [
        {
            "name": "null_optional_fields",
            "payload": {
                "messages": [{"role": "user", "content": "Test"}]
            },
            # Missing priority, timeout, retry_policy
            "expected": "Success (defaults applied)"
        },
        {
            "name": "empty_messages_array",
            "payload": {"messages": []},
            "expected": "ValidationError"
        },
        {
            "name": "empty_content",
            "payload": {"messages": [{"role": "user", "content": ""}]},
            "expected": "ValidationError"
        }
    ],
    
    "concurrent_edge_cases": [
        {
            "name": "duplicate_task_ids",
            "tasks": [
                {"task_id": "same_id", "payload": ...},
                {"task_id": "same_id", "payload": ...}
            ],
            "expected": "Second task rejected (duplicate ID)"
        },
        {
            "name": "rapid_submission",
            "tasks": [generate_task() for _ in range(1000)],
            "submit_rate": 100  # per second
            "expected": "All accepted (no dropped tasks)"
        }
    ],
    
    "state_edge_cases": [
        {
            "name": "claim_expired_task",
            "scenario": "Task already in COMPLETED state",
            "expected": "Claim fails (invalid state)"
        },
        {
            "name": "double_claim",
            "scenario": "Two workers try to claim same task",
            "expected": "Only one succeeds"
        }
    ]
}
```

---

## Chaos Data

### Failure-Inducing Payloads

```python
# chaos_data.py
CHAOS_DATA = {
    "bedrock_errors": [
        {
            "name": "throttling_429",
            "payload": {
                "model_id": "mock_throttled_model",
                "messages": [{"role": "user", "content": "Test"}]
            },
            "mock_response": {
                "status_code": 429,
                "error": "ThrottlingException"
            },
            "expected_behavior": "Retry with AIMD backoff"
        },
        {
            "name": "validation_error_400",
            "payload": {
                "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
                "messages": [{"role": "user", "content": "Test"}],
                "inference_config": {
                    "temperature": 5.0  # Out of range
                }
            },
            "expected_behavior": "Immediate failure, no retry"
        },
        {
            "name": "timeout",
            "payload": {
                "model_id": "mock_slow_model",
                "messages": [{"role": "user", "content": "Test"}]
            },
            "mock_response": {"delay": 60},  # 60 second delay
            "timeout": 5,
            "expected_behavior": "Timeout, retry"
        },
        {
            "name": "internal_error_500",
            "payload": {
                "model_id": "mock_error_model",
                "messages": [{"role": "user", "content": "Test"}]
            },
            "mock_response": {
                "status_code": 500,
                "error": "InternalServerError"
            },
            "expected_behavior": "Retry with backoff"
        }
    ],
    
    "network_failures": [
        {
            "name": "connection_timeout",
            "inject": "NetworkPartition",
            "duration": 10,  # seconds
            "expected_behavior": "Reconnect after partition heals"
        },
        {
            "name": "dns_failure",
            "inject": "DNSFailure",
            "duration": 5,
            "expected_behavior": "Retry DNS resolution"
        }
    ],
    
    "resource_exhaustion": [
        {
            "name": "memory_pressure",
            "inject": "MemoryLeak",
            "rate": "1MB/sec",
            "expected_behavior": "Graceful degradation"
        },
        {
            "name": "disk_full",
            "inject": "DiskFull",
            "threshold": "95%",
            "expected_behavior": "Error on write operations"
        }
    ]
}
```

---

## Data Refresh Strategy

### When to Refresh

| Data Type | Refresh Frequency | Trigger |
|-----------|-------------------|---------|
| **Synthetic Datasets** | Weekly | Scheduled job |
| **Edge Cases** | On schema change | Manual |
| **Chaos Scenarios** | Monthly | Scheduled job |
| **Production Stats** | Daily | Automated |

### Refresh Automation

```python
# refresh_test_data.py
import schedule
import time
from data_generator import SyntheticDataGenerator

def refresh_synthetic_data():
    """Regenerate all synthetic datasets."""
    print(f"Refreshing synthetic data: {datetime.now()}")
    
    # Use new seed based on date
    seed = int(datetime.now().strftime('%Y%m%d'))
    generator = SyntheticDataGenerator(seed=seed)
    
    # Regenerate all datasets
    datasets = {
        'smoke_test_10.json': generator.generate_dataset(10),
        'regression_100.json': generator.generate_dataset(100),
        'load_test_1000.json': generator.generate_dataset(1000),
    }
    
    for filename, data in datasets.items():
        backup_existing(filename)
        with open(f'test_data/{filename}', 'w') as f:
            json.dump(data, f, indent=2)
    
    print("Synthetic data refresh complete")

def backup_existing(filename: str):
    """Backup existing data before refresh."""
    import shutil
    src = f'test_data/{filename}'
    dst = f'test_data/backups/{filename}.{datetime.now().strftime("%Y%m%d")}'
    shutil.copy(src, dst)

# Schedule weekly refresh (Sunday 2 AM)
schedule.every().sunday.at("02:00").do(refresh_synthetic_data)

while True:
    schedule.run_pending()
    time.sleep(3600)  # Check every hour
```

---

## Security & Privacy

### PII Handling Policy

**Prohibited:**
- ❌ Real user prompts/content
- ❌ User identifiers (emails, usernames, IDs)
- ❌ API keys or credentials
- ❌ IP addresses or location data

**Allowed:**
- ✅ Synthetic generated content
- ✅ Anonymized statistical distributions
- ✅ Schema definitions
- ✅ Non-PII system metrics

### Data Classification

| Classification | Handling | Storage | Retention |
|----------------|----------|---------|-----------|
| **Public** | No restrictions | Any | Indefinite |
| **Internal** | Encrypted at rest | Approved storage | 90 days |
| **Confidential** | Encrypted + access control | Secure storage | 30 days |
| **PII** | ❌ Not allowed in tests | N/A | N/A |

### Secure Data Generation

```python
# secure_generator.py
import secrets
import hashlib

def generate_secure_id() -> str:
    """Generate cryptographically secure ID."""
    return secrets.token_urlsafe(16)

def anonymize_identifier(identifier: str, salt: str) -> str:
    """One-way hash of identifier."""
    return hashlib.sha256(f"{identifier}{salt}".encode()).hexdigest()[:16]

# Usage in tests
task_id = generate_secure_id()  # Instead of predictable "task_001"
user_id = anonymize_identifier(real_user_id, SALT)  # If needed
```

---

## Data Management

### Directory Structure

```
test_data/
├── synthetic/
│   ├── smoke_test_10.json
│   ├── regression_100.json
│   └── load_test_1000.json
├── edge_cases/
│   ├── boundary_values.json
│   ├── special_chars.json
│   └── null_empty.json
├── chaos/
│   ├── bedrock_errors.json
│   ├── network_failures.json
│   └── resource_exhaustion.json
├── schemas/
│   ├── task_schema.json
│   └── response_schema.json
├── backups/
│   └── [timestamped backups]
└── README.md
```

### Data Versioning

Use semantic versioning for test data:

```json
{
  "dataset_version": "1.2.0",
  "schema_version": "1.0.0",
  "generated_at": "2026-02-08T10:00:00Z",
  "generator_version": "1.0.0",
  "seed": 42,
  "num_tasks": 100,
  "distribution": {
    "normal": 0.7,
    "edge": 0.2,
    "invalid": 0.1
  },
  "tasks": [...]
}
```

### Loading Test Data

```python
# test_data_loader.py
import json
from pathlib import Path

class TestDataLoader:
    """Load test data for scenarios."""
    
    def __init__(self, base_path: str = "test_data"):
        self.base_path = Path(base_path)
    
    def load_dataset(self, name: str) -> list[dict]:
        """Load dataset by name."""
        path = self.base_path / "synthetic" / f"{name}.json"
        with open(path) as f:
            data = json.load(f)
        return data.get('tasks', data)
    
    def load_edge_cases(self, category: str) -> list[dict]:
        """Load edge cases by category."""
        path = self.base_path / "edge_cases" / f"{category}.json"
        with open(path) as f:
            return json.load(f)
    
    def load_chaos_scenarios(self, type: str) -> list[dict]:
        """Load chaos scenarios."""
        path = self.base_path / "chaos" / f"{type}.json"
        with open(path) as f:
            return json.load(f)

# Usage in tests
@pytest.fixture
def test_data():
    return TestDataLoader()

def test_happy_path(test_data):
    tasks = test_data.load_dataset("smoke_test_10")
    for task in tasks:
        submit_task(task)
        # ...
```

---

## Summary

This test data strategy provides:

✅ **Synthetic-First:** Generate realistic data without PII risks  
✅ **Comprehensive Coverage:** Normal, edge, invalid, chaos cases  
✅ **Reproducible:** Seeded generators for deterministic tests  
✅ **Privacy-Preserving:** No real user data in tests  
✅ **Automated:** Scheduled refresh and versioning  
✅ **Secure:** Cryptographic IDs and anonymization  

**Next Document:** Test Environments (05_test_environments.md)

---

## References

- [Faker Documentation](https://faker.readthedocs.io/)
- [Test Data Management](https://martinfowler.com/bliki/TestDataBuilder.html)
- [Synthetic Data Generation](https://www.synthetics.com/)
- [GDPR Test Data](https://gdpr.eu/test-data/)

---

**Document Status:** ✅ Complete  
**Review Date:** 2026-02-15  
**Next Review:** 2026-05-08
