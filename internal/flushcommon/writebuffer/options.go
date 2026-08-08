package writebuffer

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type WriteBufferOption func(opt *writeBufferOption)

type TaskObserverCallback func(t syncmgr.Task, err error)

type FlushSourceModeNotifier func(segmentID int64, mode metacache.FlushSourceMode)

// GrowingSourceResolver resolves an optional in-memory growing segment source
// for growing-source flush. GrowingSourcePending means the growing source exists but has not
// caught up to targetOffset yet; WriteBuffer should only be used when the state
// is GrowingSourceUnavailable.
type GrowingSourceResolver func(segmentID int64, targetOffset int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState)

type writeBufferOption struct {
	ctx          context.Context
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	pkStatsFactory       metacache.PkStatsFactory
	metaWriter           syncmgr.MetaWriter
	errorHandler         func(error)
	taskObserverCallback TaskObserverCallback
	storageVersion       int64

	growingSourceResolver      GrowingSourceResolver
	growingSourceRetryInterval time.Duration
	flushSourceModeNotifier    FlushSourceModeNotifier
}

func defaultWBOption(metacache metacache.MetaCache) *writeBufferOption {
	return &writeBufferOption{
		ctx: context.Background(),
		syncPolicies: []SyncPolicy{
			GetFullBufferPolicy(),
			GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
			GetSealedSegmentsPolicy(metacache),
			GetDroppedSegmentPolicy(metacache),
		},
		// default error handler, just panicking
		errorHandler: func(err error) {
			panic(err)
		},
	}
}

func WithIDAllocator(allocator allocator.Interface) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.idAllocator = allocator
	}
}

func WithPKStatsFactory(factory metacache.PkStatsFactory) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.pkStatsFactory = factory
	}
}

func WithMetaWriter(writer syncmgr.MetaWriter) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.metaWriter = writer
	}
}

func WithSyncPolicy(policy SyncPolicy) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.syncPolicies = append(opt.syncPolicies, policy)
	}
}

func WithErrorHandler(handler func(err error)) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.errorHandler = handler
	}
}

// WithContext sets the lifecycle context for the write buffer. Background
// retries inside the buffer (e.g. L0 segment ID allocation) are bounded by this
// context, so they stop when the channel/node is shutting down.
func WithContext(ctx context.Context) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.ctx = ctx
	}
}

// WithTaskObserverCallback sets the callback function for observing task status.
// The callback will be called when every task is executed, should be concurrent safe to be called.
func WithTaskObserverCallback(callback TaskObserverCallback) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.taskObserverCallback = callback
	}
}

func WithGrowingSourceResolver(resolver GrowingSourceResolver) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.growingSourceResolver = resolver
	}
}

func WithFlushSourceModeNotifier(notifier FlushSourceModeNotifier) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.flushSourceModeNotifier = notifier
	}
}
