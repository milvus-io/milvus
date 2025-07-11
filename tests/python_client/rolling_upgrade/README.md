# Milvus Rolling Upgrade Tests

This directory contains automated tests for validating Milvus rolling upgrade functionality, ensuring that clusters can be upgraded to new versions without service disruption.

## Overview

Rolling upgrade tests verify that Milvus can perform seamless version upgrades while maintaining:
- Continuous service availability
- Data integrity
- Minimal performance impact
- Client operation compatibility

## Test Files

### Core Test Suites

1. **test_rolling_update_by_default.py**
   - Tests default rolling upgrade using Kubernetes CRD
   - Updates all components simultaneously
   - Validates cluster health and readiness

2. **test_rolling_update_one_by_one.py**
   - Tests granular component-by-component upgrades
   - Default order: indexNode → rootCoord → dataCoord/indexCoord → queryCoord → queryNode → dataNode → proxy
   - Supports pausing/resuming specific components

### Operation Tests

1. **testcases/test_concurrent_request_operation_for_rolling_update.py**
   - Validates system behavior under concurrent load during upgrades
   - Tests insert, search, query, and delete operations
   - Monitors success rates (>98% threshold) and RTO

2. **testcases/test_single_request_operation_for_rolling_update.py**
   - Tests individual operation types during upgrades
   - Includes collection creation, flush, and index operations
   - Tracks per-operation metrics and failures

### Utilities

- **monitor_rolling_update.py**: Standalone pod monitoring utility
- **conftest.py**: Test configuration and fixtures

## Configuration

### Milvus CRD Files

- **milvus_crd.yaml**: Standard cluster configuration
- **milvus_mixcoord_crd.yaml**: MixCoord deployment configuration

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `new_image_repo` | Target image repository | - |
| `new_image_tag` | Target version tag | - |
| `components_order` | Component update sequence | See test files |
| `paused_components` | Components to pause during update | [] |
| `paused_duration` | Pause duration in seconds | 300 |
| `request_duration` | Operation test duration | 1800 |
| `is_check` | Enforce success rate assertions | True |

## Running Tests

### Prerequisites

1. Kubernetes cluster with Milvus operator installed
2. kubectl configured with cluster access
3. Python environment with required dependencies

### Basic Usage

```bash
# Run default rolling upgrade test
pytest test_rolling_update_by_default.py -v

# Run one-by-one upgrade test
pytest test_rolling_update_one_by_one.py -v

# Run with custom parameters
pytest test_rolling_update_one_by_one.py \
  --new_image_repo=milvusdb/milvus \
  --new_image_tag=v2.4.1 \
  --paused_components=queryNode \
  --paused_duration=600
```

### Monitor Pod Status

```bash
# Monitor pods for 10 minutes with 5-second intervals
python monitor_rolling_update.py --duration 600 --interval 5 --release my-release
```

## Success Criteria

### During Upgrade
- **Success Rate**: ≥98% for all operations
- **RTO**: ≤10 seconds per operation type
- **Pod Status**: All pods eventually reach Ready state

### After Upgrade
- **Success Rate**: 100% for all operations
- **Cluster Health**: All components healthy
- **Data Integrity**: No data loss or corruption

## Test Results

Test results are saved in parquet format:
- `upgrade_result_<operation>_<timestamp>.parquet`
- Contains detailed metrics, failed requests, and RTO data

## Architecture

```
Rolling Upgrade Process:
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Old Image  │ --> │  Upgrading  │ --> │  New Image  │
└─────────────┘     └─────────────┘     └─────────────┘
       │                    │                    │
       v                    v                    v
  [Running Pods]      [Mixed Pods]         [Updated Pods]
       │                    │                    │
       └────────────────────┴────────────────────┘
                    Continuous Operations
```
