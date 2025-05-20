# Go MilvusClient

[![license](https://img.shields.io/hexpm/l/plug.svg?color=green)](https://github.com/milvus-io/milvus/blob/master/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/milvus-io/milvus/client/v2.svg)](https://pkg.go.dev/github.com/milvus-io/milvus/client/v2)

Go MilvusClient for [Milvus](https://github.com/milvus-io/milvus). To contribute code to this project, please read our [contribution guidelines](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md) first.


## Getting started

### Prerequisites

Go 1.24.2 or higher

### Install Milvus Go SDK

1. Use `go get` to install the latest version of the Milvus Go SDK and dependencies:

   ```shell
   go get -u github.com/milvus-io/milvus/client/v2
   ```

2. Include the Go MilvusClient in your application:

   ```go
    import "github.com/milvus-io/milvus/client/v2/milvusclient"

    //...other snippet ...
    ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	milvusAddr := "YOUR_MILVUS_ENDPOINT"

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
    }

    // Do your work with milvus client
    ```

### API Documentation

Refer to [https://milvus.io/api-reference/go/v2.5.x/About.md](https://milvus.io/api-reference/go/v2.5.x/About.md) for the Go SDK API documentation.

## Code format

The Go source code is formatted using gci & gofumpt. Please run `make lint-fix` before sumbit a PR.
