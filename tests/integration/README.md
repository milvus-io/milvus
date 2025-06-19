# Integration test

This folder contains the integration test for Milvus components.


## How it works

The Milvus integration test framework is a comprehensive testing solution that runs multiple Milvus components as separate processes to simulate a real deployment environment. It provides a `MiniClusterV3` that manages the lifecycle of core Milvus components including MixCoord, Proxy, DataNode, QueryNode and StreamingNode.

The framework allows developers to:
- Start/stop a new milvus cluster
- Start/stop individual Milvus components
- Monitor component states and metadata through etcd
- Execute end-to-end test scenarios
- Execute the method of any component from its client
- Simulate component failures and recovery
- Modify the milvus configration at runtime or startup

The test framework is built on top of Go's testing package and the testify/suite framework, making it easy to write structured and maintainable integration tests.

## How to run integration test locally

Because integration test is a multi-process framework, it requires some components to start:

- a built milvus binary
- etcd
- minio
- pulsar

Build the milvus binary first.

```base
make milvus 

# test framework will use the env `MILVUS_WORK_DIR` to find the milvus binary.
# already done in the scripts/setenv.sh
# or you can set it manually
export MILVUS_WORK_DIR=$(pwd) 
```

Run the docker compose to start the etcd, minio and pulsar.

```bash
cd [milvus-folder]/deployments/docker/dev && docker compose up -d
```

Run the integration test.

```base
make integration-test
```

If you want to run single test case, you could execute command like this example

```bash
# mq, etcd, minio ready before
cd [milvus-folder]
source scripts/setenv.sh
cd tests/integration/[testcase-folder]/
go test -run "$testCaseName^" -testify.m "$subTestifyCaseName^" -race -v
```

## Recommended coding style for add new cases

### Using `suite`

MiniCluster` and `MiniClusterSuite` provides lots of comment preset tool function to execute intergration test.

It is recommend to add a new test with `testify/suite`

```go

import (
    // ...
    "github.com/milvus-io/milvus/tests/integration"
)

type NewSuite struct {
    integration.MiniClusterSuite
}


// Setups and teardowns, optional if no custom logic needed
// example to suite setup & teardown, same logic applies to test setup&teardown

func (s *NewSuite) SetupSuite() {
    s.MiniClusterSuite.SetupSuite()
    // customized setup
}

func (s *NewSuite) TearDownSuite() {
    s.MiniClusterSuite.TearDownSuite()
    // customized teardown
}

```

A suite will start a new empty milvus cluster, and the cluster will be reused for all test cases in the suite.

### New folder for each new scenario

It's a known issue that integration test cases run in same process might affect due to some singleton component not fully cleaned.

As a temp solution, test cases are separated into different packages to run independently. 
