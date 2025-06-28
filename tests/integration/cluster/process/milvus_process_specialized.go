package process

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
)

type (
	MixcoordProcess  = specializedMilvusProcess[types.MixCoordClient]
	ProxyProcess     = specializedMilvusProcess[types.ProxyClient]
	DataNodeProcess  = specializedMilvusProcess[types.DataNodeClient]
	QueryNodeProcess = specializedMilvusProcess[types.QueryNodeClient]
	// StreamingNodeProcess is a special query node process by now.
	StreamingNodeProcess = specializedMilvusProcess[types.QueryNodeClient]
)

// NewMixCoordProcess creates a new MixcoordProcess.
func NewMixCoordProcess(opts ...Option) *MixcoordProcess {
	opts = append(opts, WithRole("mixcoord"))
	mp := NewMilvusProcess(opts...)
	return &MixcoordProcess{
		MilvusProcess: mp,
	}
}

// NewProxyProcess creates a new ProxyProcess.
func NewProxyProcess(opts ...Option) *ProxyProcess {
	opts = append(opts, WithRole("proxy"))
	mp := NewMilvusProcess(opts...)
	return &ProxyProcess{
		MilvusProcess: mp,
	}
}

// NewQueryNodeProcess creates a new QueryNodeProcess.
func NewQueryNodeProcess(opts ...Option) *QueryNodeProcess {
	opts = append(opts, WithRole("querynode"))
	mp := NewMilvusProcess(opts...)
	return &QueryNodeProcess{
		MilvusProcess: mp,
	}
}

// NewStreamingNodeProcess creates a new StreamingNodeProcess.
func NewStreamingNodeProcess(opts ...Option) *StreamingNodeProcess {
	opts = append(opts, WithRole("streamingnode"))
	mp := NewMilvusProcess(opts...)
	return &StreamingNodeProcess{
		MilvusProcess: mp,
	}
}

// NewDataNodeProcess creates a new DataNodeProcess.
func NewDataNodeProcess(opts ...Option) *DataNodeProcess {
	opts = append(opts, WithRole("datanode"))
	mp := NewMilvusProcess(opts...)
	return &DataNodeProcess{
		MilvusProcess: mp,
	}
}

// specializedMilvusProcess is a specialized Milvus process that returns a client of type T.
type specializedMilvusProcess[T any] struct {
	*MilvusProcess
}

// GetClient returns a client of type T.
func (mp *specializedMilvusProcess[T]) MustGetClient(ctx context.Context) T {
	client, err := mp.MilvusProcess.GetClient(ctx)
	if err != nil {
		panic(err)
	}
	return client.(T)
}

// MustWaitForReady waits for the Milvus process to be ready.
func (mp *specializedMilvusProcess[T]) MustWaitForReady(ctx context.Context) {
	if err := mp.MilvusProcess.waitForReady(ctx); err != nil {
		panic(err)
	}
}
