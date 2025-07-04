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

### Should I add a new test case into integration?

It's a good choice to add a new test case into integration test if:

- The test case need to control the lifecycle of Milvus components, such as starting/stopping components or modifying configuration then executing some end-to-end scenarios.
- If the test case is hard to apply in the unit-test and E2E test, such as testing in a non-default configured Milvus cluster, and need to be tested between multiple components.

Should not add a new test case into integration test if: 

- Function-verification that can be covered by unit test or already be covered by E2E test.
- Performance test.

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

A suite will start a new empty milvus cluster, and the cluster will be reused for all test cases in the suite. We recommend to add more useful utility methods into `MiniClusterSuite` or `MilvusClusterV3` to interact with the cluster, to speed up the integration test development.
Some utility methods are provided in `MiniClusterSuite` to interact with the cluster:

#### method of `MiniClusterSuite`


- Use `s.WithMilvusConfig` to modify the milvus configuration at startup in `SetupSuite` method.
- Use `s.WithOptions` to modify the test options at startup in `SetupSuite` method.
- Use `s.Cluster` to get the `MiniClusterV3` instance, which provides methods to interact with the Milvus cluster.
- Some useful milvus method is provided in `util_` files, such as `CreateCollection`, `Insert`, `Flush`..., 

#### method of `MiniClusterV3`

- Use `s.Cluster.MustModifyMilvusConfig` to modify the milvus configuration at runtime, it will return a guard function to restore the modified configuration. It doesn't promise that the configuration will be applied immediately, milvus may not support the dynamic configuration change for some configurations or some configration may be applied slowly.
- Use `s.Cluster.Add*` to add components to the cluster, such as `AddMixCoord`, `AddProxy`, `AddDataNode`, `AddQueryNode`, `AddStreamingNode`. it will return the `MilvusProcess` object to manage the lifetime of new incoming component. It will block until the component is healthy by default, use `WithoutWaitForReady` option to avoid it.
- Use `s.Cluster.Default*` to get the default component, such as `DefaultMixCoord`, `DefaultProxy`, `DefaultDataNode`, `DefaultQueryNode`, `DefaultStreamingNode`.
- Use `s.*Client` to get the grpc client of the default component that can be got from `s.Cluster.Default*`, such as `s.MixCoordClient`, `s.ProxyClient`, `s.DataNodeClient`, `s.QueryNodeClient`, `s.StreamingNodeClient`.
- Use `s.MilvusClient` to get the grpc client of the Milvus server, which is connnected to the proxy that is returned from `DefaultProxy()`.

#### method of `MilvusProcess`

- Use `p.MustGetClient` to get the grpc client of the component, which provides methods to interact with the component.
- Use `p.MustWaitForReady` to wait for the component to be ready, it will block until the component is healthy.
- Use `p.Stop` to stop the component, it will block until the component is stopped. It will perform a graceful shutdown by default. When the given deadline is excceed, `ForceStop` is performed.
- Use `p.ForceStop` to force stop the component, it will not wait for the component to be stopped.
- Use `p.IsWorking` to check if the component is working, it will return false if the component is stopped.

### New folder for each new scenario

It's a known issue that integration test cases run in same process might affect due to some singleton component not fully cleaned.

As a temp solution, test cases are separated into different packages to run independently. 

### Some tips

1. Sometimes, if the test case is killed by some SIGKILL, it will leave some orphan milvus process running in the background. You could use `killall milvus` to kill all milvus process, or `killall -9 milvus` to kill all milvus process forcefully. 
2. Because the test framework use some determined port (such as 53100 for coord, 19530 for proxy), it will be failed to start a new milvus process if the port is already in use. You could use `lsof -i :53100` to check if the port is already in use.
3. The test coverage of milvus can not be generated by the integration test, because that the integration test use multi-process. the test coverage only cover the code that is executed by the integration test itself.
