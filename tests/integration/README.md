# Integration test

This folder contains the integration test for Milvus components.

## How to run integration test locally

Integration test still need some thirdparty components to start:

```bash
cd [milvus-folder]/deployments/docker/dev && docker compose up -d
```

Run following script to start the full integration test:
```bash
cd [milvus-folder]
make milvus # milvus needs to be compiled to make cpp build ready
./scripts/run_intergration_test.sh
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

### New folder for each new scenario

It's a known issue that integration test cases run in same process might affect due to some singleton component not fully cleaned.

As a temp solution, test cases are separated into different packages to run independently. 
