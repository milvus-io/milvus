package writebuffer

import (
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
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	pkStatsFactory metacache.PkStatsFactory
	metaWriter     syncmgr.MetaWriter
	// errorHandler terminates the process. It is the last resort for a sync
	// failure the write buffer cannot recover from: the rows have already been
	// yielded out of the buffer, so if nobody re-runs the task the data is
	// gone from memory while the checkpoint stays pinned — a silent, signal-less
	// stall that is strictly worse than a crash. Only wire a non-fatal handler
	// for a task type that owns a re-submit path.
	errorHandler func(error)
	// growingSourceErrorHandler handles growing-source sync failures that the
	// write buffer WILL retry (see scheduleGrowingSourceRetryLocked). Those
	// rows live in the segcore growing segment, not in a yielded buffer, so a
	// failed attempt loses nothing and the next attempt re-reads the same
	// offset range. Defaults to record-and-continue.
	growingSourceErrorHandler func(error)
	taskObserverCallback      TaskObserverCallback
	storageVersion            int64

	growingSourceResolver      GrowingSourceResolver
	growingSourceRetryInterval time.Duration
	flushSourceModeNotifier    FlushSourceModeNotifier
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
	// growingSourceErrorHandler is deliberately left nil: newWriteBufferBase
	// installs a rate-limited handler once the channel logger exists. Setting a
	// plain mlog.Warn here would win over that fallback and, at a 100ms retry
	// interval, emit ~10 unrated warnings per second per stuck segment.
}

// WithGrowingSourceErrorHandler overrides the growing-source failure handler.
func WithGrowingSourceErrorHandler(handler func(err error)) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.growingSourceErrorHandler = handler
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
