package wal

import "github.com/milvus-io/milvus/internal/util/streamingutil/status"

// TransformLogProvider is the integration boundary between the local SN
// implementation and the remote subscription service. The SN workspace owns
// its implementation, including availability, coverage, retention and lifetime.
type TransformLogProvider interface {
	TransformLog() TransformLogAccesser
}

func TransformLogFor(w ROWAL) TransformLogAccesser {
	if provider, ok := w.(TransformLogProvider); ok {
		return provider.TransformLog()
	}
	return NewTransformLogErrorAccesser(status.NewUnrecoverableError("WAL does not support transform subscriptions"))
}
