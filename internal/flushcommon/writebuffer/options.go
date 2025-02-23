package writebuffer

import (
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type WriteBufferOption func(opt *writeBufferOption)

type TaskObserverCallback func(t syncmgr.Task, err error)

type writeBufferOption struct {
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	pkStatsFactory       metacache.PkStatsFactory
	metaWriter           syncmgr.MetaWriter
	errorHandler         func(error)
	taskObserverCallback TaskObserverCallback
}

func defaultWBOption(metacache metacache.MetaCache) *writeBufferOption {
	return &writeBufferOption{
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

// WithTaskObserverCallback sets the callback function for observing task status.
// The callback will be called when every task is executed, should be concurrent safe to be called.
func WithTaskObserverCallback(callback TaskObserverCallback) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.taskObserverCallback = callback
	}
}
