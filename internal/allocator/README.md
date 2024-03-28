# ID Allocator

This package provides an ID allocator that generates unique and monotonically increasing IDs. It supports both global ID allocation using a global allocator and local ID allocation with a cached allocator.

## Features

- Allocate unique and monotonically increasing IDs
- Global ID allocation using a global allocator
- Local ID allocation with a cached allocator
- Support for batch allocation of IDs
- Synchronization with the global allocator
- Graceful shutdown and resource cleanup

## Usage

### GlobalIDAllocator

The `GlobalIDAllocator` is a global single point TSO allocator that generates unique and monotonically increasing IDs. It uses an underlying KV store to persist the allocated IDs.

```go
import (
    "github.com/milvus-io/milvus/internal/allocator"
    "github.com/milvus-io/milvus/internal/kv"
)

// Create a new GlobalIDAllocator
allocator := allocator.NewGlobalIDAllocator("idTimestamp", etcdKV)

// Initialize the allocator
err := allocator.Initialize()

// Allocate a single ID
id, err := allocator.AllocOne()

// Allocate a batch of IDs
count := uint32(1000)
startID, endID, err := allocator.Alloc(count)
```

### IDAllocator

The `IDAllocator` is a cached allocator that allocates IDs from a remote global allocator. It batches the allocation requests to reduce the number of remote calls.

```go
import (
    "context"
    "github.com/milvus-io/milvus/internal/allocator"
)

// Create a new IDAllocator
remoteAllocator := // Create a remote allocator instance
peerID := // Set the peer ID
allocator, err := allocator.NewIDAllocator(context.Background(), remoteAllocator, peerID)

// Start the allocator
err = allocator.Start()

// Allocate a single ID
id, err := allocator.AllocOne()

// Allocate a batch of IDs
count := uint32(1000)
startID, endID, err := allocator.Alloc(count)

// Close the allocator
allocator.Close()
```

## Configuration

The ID allocator can be configured using the following parameters:

- `idCountPerRPC`: The number of IDs to allocate per RPC call to the remote allocator (default: 200000).
- `maxConcurrentRequests`: The maximum number of concurrent allocation requests (default: 10000).

## Testing

The package includes unit tests to verify the correctness of the ID allocator. To run the tests:

```shell
go test ./...
```

## License

This package is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for more information.
