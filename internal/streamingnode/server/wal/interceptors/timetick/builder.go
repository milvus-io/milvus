package timetick

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ interceptors.InterceptorBuilder = (*interceptorBuilder)(nil)

// NewInterceptorBuilder creates a new interceptor builder.
// 1. Add timetick to all message before append to wal.
// 2. Collect timetick info, and generate sync-timetick message to wal.
func NewInterceptorBuilder() interceptors.InterceptorBuilder {
	return &interceptorBuilder{}
}

// interceptorBuilder is a builder to build timeTickAppendInterceptor.
type interceptorBuilder struct{}

// Build implements Builder.
func (b *interceptorBuilder) Build(param interceptors.InterceptorBuildParam) interceptors.BasicInterceptor {
	ctx, cancel := context.WithCancel(context.Background())
	interceptor := &timeTickAppendInterceptor{
		ctx:        ctx,
		cancel:     cancel,
		ready:      make(chan struct{}),
		ackManager: ack.NewAckManager(),
		ackDetails: &ackDetails{},
		sourceID:   paramtable.GetNodeID(),
	}
	go interceptor.executeSyncTimeTick(
		// TODO: move the configuration to streamingnode.
		paramtable.Get().ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond),
		param,
	)
	return interceptor
}
