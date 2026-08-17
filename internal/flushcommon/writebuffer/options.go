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

// GrowingSourceResolver is the ONE authority for growing-source flush on a
// channel: segment resolution (GetGrowingFlushSource) AND the provider
// registration tokens the admission fence records and re-checks
// (LatestRegistrationToken).
//
// The two MUST come from the same authority. An injected resolver that never
// registered with the global registry would make the fence record token 0 =
// "never fenced" = admission never closes — the seam and the fence have to
// agree on who the provider is, so both methods live on one interface.
//
// GetGrowingFlushSource: GrowingSourcePending means the growing source exists
// but has not consumed up to endPos yet; WriteBuffer should only be used when
// the state is GrowingSourceUnavailable.
type GrowingSourceResolver interface {
	GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState)
	LatestRegistrationToken(channel string) uint64
}

// GrowingSourceResolverFunc adapts a bare resolve function (tests) into a
// GrowingSourceResolver. Its token authority stays the process-global registry
// — exactly what the fence consulted before the seam existed — so an injected
// func resolver keeps today's fence behavior; a resolver with its OWN
// registration lifecycle must implement the full interface instead.
type GrowingSourceResolverFunc func(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState)

func (f GrowingSourceResolverFunc) GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return f(segmentID, endPos)
}

func (f GrowingSourceResolverFunc) LatestRegistrationToken(channel string) uint64 {
	return syncmgr.DefaultGrowingSourceRegistry().LatestRegistrationToken(channel)
}

// registryGrowingSourceResolver is the default resolver: the process-local
// growing source registry backs both resolution and registration tokens. If a
// registry lookup misses, growing-source data falls back to WriteBuffer.
type registryGrowingSourceResolver struct {
	channel string
}

func (r registryGrowingSourceResolver) GetGrowingFlushSource(segmentID int64, endPos *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return syncmgr.DefaultGrowingSourceRegistry().Resolve(r.channel, segmentID, endPos)
}

func (registryGrowingSourceResolver) LatestRegistrationToken(channel string) uint64 {
	return syncmgr.DefaultGrowingSourceRegistry().LatestRegistrationToken(channel)
}

type writeBufferOption struct {
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	metaWriter syncmgr.MetaWriter
	// errorHandler terminates the process. It is the last resort for a sync
	// failure the write buffer cannot recover from: the rows have already been
	// yielded out of the buffer, so if nobody re-runs the task the data is
	// gone from memory while the checkpoint stays pinned — a silent, signal-less
	// stall that is strictly worse than a crash. Only wire a non-fatal handler
	// for a task type that owns a re-submit path.
	errorHandler func(error)
	// growingSourceErrorHandler handles growing-source sync failures that the
	// write buffer WILL retry (see armRefRetryLocked). Those
	// rows live in the segcore growing segment, not in a yielded buffer, so a
	// failed attempt loses nothing and the next attempt re-reads the same
	// timestamp range. Defaults to record-and-continue.
	growingSourceErrorHandler func(error)
	taskObserverCallback      TaskObserverCallback

	// growingSourceResolver is a test-only override; the func shape keeps test
	// literals terse while the write buffer itself holds the full interface.
	growingSourceResolver   GrowingSourceResolverFunc
	flushRetryInterval      time.Duration
	flushSourceModeNotifier FlushSourceModeNotifier
}

func defaultWBOption(metacache metacache.MetaCache) *writeBufferOption {
	return &writeBufferOption{
		syncPolicies: []SyncPolicy{
			GetFullBufferPolicy(),
			GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
			GetSealedSegmentsPolicy(metacache),
			GetDroppedSegmentPolicy(metacache),
		},
	}
	// errorHandler is deliberately left nil: newWriteBufferBase installs the
	// panicking default when the option carries none, so this constructor does
	// not need to duplicate it.
	//
	// growingSourceErrorHandler is deliberately left nil: newWriteBufferBase
	// installs a rate-limited handler once the channel logger exists. Setting a
	// plain mlog.Warn here would win over that fallback and, at a 100ms retry
	// interval, emit ~10 unrated warnings per second per stuck segment.
}

func WithIDAllocator(allocator allocator.Interface) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.idAllocator = allocator
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

func WithFlushSourceModeNotifier(notifier FlushSourceModeNotifier) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.flushSourceModeNotifier = notifier
	}
}
