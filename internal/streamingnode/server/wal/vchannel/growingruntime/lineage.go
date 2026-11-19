package growingruntime

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The residual of a split source is the set of its segments that this runtime
// still serves: segments sealed by the fence that have not yet been handed to
// QueryNodes. After the fence a delete of one of their rows is written to a
// target of the split, so the runtime follows the TransformLog stream of every
// target from T_switch, applies its delete entries to the residual and keeps
// an applied position per target. A query of a lineage group waits for those
// positions. The subscription ends when the runtime holds no segment any more.

const (
	lineageRetryMinDelay = 100 * time.Millisecond
	lineageRetryMaxDelay = 5 * time.Second
)

var errLineageSubscriptionEnded = errors.New("residual lineage subscription ended")

// lineageSpec is the split lineage of the vchannel: the targets that took its
// DML, and T_switch.
type lineageSpec struct {
	vchannels      []string
	switchTimeTick uint64
}

func lineageSpecFromMeta(lineage *streamingpb.VChannelSplitLineage) *lineageSpec {
	if lineage == nil || len(lineage.GetTargetVchannels()) == 0 {
		return nil
	}
	return &lineageSpec{
		vchannels:      slices.Clone(lineage.GetTargetVchannels()),
		switchTimeTick: lineage.GetSwitchTimeTick(),
	}
}

func lineageSpecFromFence(msg message.ImmutableMessage) *lineageSpec {
	header := message.MustAsImmutableSplitShardMessageV2(msg).Header()
	if len(header.GetTargetVchannels()) == 0 {
		return nil
	}
	return &lineageSpec{
		vchannels:      slices.Clone(header.GetTargetVchannels()),
		switchTimeTick: msg.TimeTick(),
	}
}

// residualLineage is one subscription of the residual to the target streams.
type residualLineage struct {
	spec lineageSpec

	// Guarded by Runtime.mu.
	applied map[string]uint64
	done    bool
	err     error

	// applyMu orders the application of one entry with the merge of segments
	// from a partition refresh. Staged segments receive the live suffix while
	// the captured prefix is replayed without holding this lock.
	applyMu        sync.Mutex
	replaySegments []*growingSegment

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newResidualLineage(parent context.Context, spec lineageSpec) *residualLineage {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	l := &residualLineage{
		spec:    spec,
		applied: make(map[string]uint64, len(spec.vchannels)),
		ctx:     ctx,
		cancel:  cancel,
	}
	for _, vchannel := range spec.vchannels {
		l.applied[vchannel] = spec.switchTimeTick
	}
	return l
}

// stop ends every stream and waits for their goroutines. It must not be called
// from a stream goroutine or under Runtime.mu.
func (l *residualLineage) stop() {
	if l == nil {
		return
	}
	l.cancel()
	l.wg.Wait()
}

// observeSplitLineage records the split lineage of the vchannel and starts the
// residual subscription, once. A runtime that is a partition refresh stage
// never subscribes: its segments are merged into a runtime that does.
func (r *Runtime) observeSplitLineage(spec *lineageSpec) {
	if r == nil || spec == nil || r.stage {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.lineageSpec != nil {
		return
	}
	r.lineageSpec = spec
	r.startLineageLocked()
}

// startLineageLocked starts a subscription from T_switch, or records an
// already finished one when the runtime holds no segment. Caller holds r.mu.
func (r *Runtime) startLineageLocked() {
	l := newResidualLineage(r.view.RuntimeContext, *r.lineageSpec)
	r.lineage = l
	if len(r.segments) == 0 {
		l.done = true
		l.cancel()
		r.mvccCond.Broadcast()
		return
	}
	if r.lineageStreams == nil {
		l.err = merr.WrapErrServiceInternalMsg("split source has no TransformLog streams for its targets")
		l.cancel()
		r.mvccCond.Broadcast()
		return
	}
	for _, vchannel := range l.spec.vchannels {
		l.wg.Add(1)
		go r.runLineageStream(l, r.lineageStreams, vchannel)
	}
	mlog.Info(l.ctx, "residual of a split source subscribed to its targets",
		mlog.Strings("targets", l.spec.vchannels),
		mlog.Uint64("switchTimeTick", l.spec.switchTimeTick),
		mlog.Int("segmentCount", len(r.segments)))
}

// finishLineageIfResidualGoneLocked ends the subscription once the runtime
// holds no segment: every residual segment was handed off and released, so no
// query of this runtime can read a row of it. Caller holds r.mu.
func (r *Runtime) finishLineageIfResidualGoneLocked() {
	l := r.lineage
	if l == nil || l.done || len(r.segments) != 0 {
		return
	}
	l.done = true
	r.mvccCond.Broadcast()
	go l.stop()
	mlog.Info(l.ctx, "residual of a split source released, lineage subscription ends",
		mlog.Strings("targets", l.spec.vchannels))
}

func (r *Runtime) runLineageStream(l *residualLineage, streams wal.TransformLogStreamManager, vchannel string) {
	defer l.wg.Done()
	delay := lineageRetryMinDelay
	for l.ctx.Err() == nil {
		err := r.followLineageStream(l, streams, vchannel)
		if l.ctx.Err() != nil {
			return
		}
		var overflow *lineageQueueOverflow
		if errors.As(err, &overflow) {
			for r.lineageApplied(l, vchannel) < overflow.through {
				after, replayErr := wal.ReplayTransformPage(l.ctx, streams, vchannel, r.lineageApplied(l, vchannel), overflow.through, func(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
					r.applyLineageEvent(l, vchannel, wal.TransformLogStreamEvent{Entry: entry})
					return nil
				})
				if replayErr != nil {
					err = replayErr
					break
				}
				r.applyLineageEvent(l, vchannel, wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: after}})
			}
		}
		if errors.Is(err, wal.ErrTransformLogStartPointTruncated) || errors.Is(err, wal.ErrTransformLogInvalidReadOption) || status.AsStreamingError(err).IsUnrecoverable() {
			r.mu.Lock()
			l.err = merr.Wrap(err, "cannot replay split target deletes for the residual")
			r.mvccCond.Broadcast()
			r.mu.Unlock()
			l.cancel()
			return
		}
		mlog.Warn(l.ctx, "residual lineage stream broke, resubscribing",
			mlog.FieldVChannel(vchannel),
			mlog.Uint64("startAfterTimeTick", r.lineageApplied(l, vchannel)),
			mlog.Duration("delay", delay),
			mlog.Err(err))
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-l.ctx.Done():
			timer.Stop()
			return
		}
		delay = min(delay*2, lineageRetryMaxDelay)
	}
}

// followLineageStream subscribes once, from the applied position, and applies
// events until the stream or the subscription ends.
func (r *Runtime) followLineageStream(l *residualLineage, streams wal.TransformLogStreamManager, vchannel string) error {
	ctx, cancel := context.WithCancel(l.ctx)
	defer cancel()
	stream, err := streams.AcquireStream(ctx, funcutil.ToPhysicalChannel(vchannel))
	if err != nil {
		return err
	}
	defer func() { cancel(); _ = stream.Close() }()
	handler := newLineageHandler(ctx)
	defer handler.abort()
	subscription, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           vchannel,
		StartAfterTimeTick: r.lineageApplied(l, vchannel),
		RequireExactStart:  true,
		Handler:            handler,
	})
	if err != nil {
		return err
	}
	defer func() { cancel(); _ = subscription.Close() }()
	for {
		select {
		case <-handler.done:
			if err := handler.Error(); err != nil {
				return err
			}
			if err := stream.Error(); err != nil {
				return err
			}
			return errLineageSubscriptionEnded
		case queued := <-handler.events:
			r.applyLineageEvent(l, vchannel, queued.event)
			handler.release(queued.bytes)
		case <-stream.Done():
			if err := stream.Error(); err != nil {
				return err
			}
			return errLineageSubscriptionEnded
		case <-l.ctx.Done():
			return l.ctx.Err()
		}
	}
}

// applyLineageEvent applies a delete entry to the residual or records
// payload-free progress, and raises the applied position of the stream.
func (r *Runtime) applyLineageEvent(l *residualLineage, vchannel string, event wal.TransformLogStreamEvent) {
	l.applyMu.Lock()
	defer l.applyMu.Unlock()
	var tick uint64
	switch {
	case event.Entry != nil:
		tick = event.Entry.GetTimeTick()
		if tick <= r.lineageApplied(l, vchannel) {
			return
		}
		if err := r.applyTransformLogEntryTo(l.ctx, event.Entry, append(r.segmentsSnapshot(), l.replaySegments...)); err != nil {
			// A partially applied entry cannot be retried on these segments, as
			// for a delete of the vchannel's own WAL.
			panic(merr.Wrap(err, "failed to apply a target delete to the residual of a split source"))
		}

	case event.SyncUp != nil:
		tick = event.SyncUp.TimeTick
	default:
		return
	}
	r.mu.Lock()
	if tick > l.applied[vchannel] {
		l.applied[vchannel] = tick
		r.mvccCond.Broadcast()
	}
	r.mu.Unlock()
}

func (r *Runtime) lineageApplied(l *residualLineage, vchannel string) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return l.applied[vchannel]
}

// LineageAppliedTimeTicks returns the applied position of every target stream,
// or nil when the vchannel has no split lineage.
func (r *Runtime) LineageAppliedTimeTicks() map[string]uint64 {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.lineage == nil {
		return nil
	}
	ticks := make(map[string]uint64, len(r.lineage.applied))
	for vchannel, tick := range r.lineage.applied {
		ticks[vchannel] = tick
	}
	return ticks
}

// WaitLineageVisible waits until the residual applied every delete of each
// target stream through its requested tick. It returns at once when the
// residual has been released: the runtime then holds no row a target delete
// could hide. A request that names a vchannel this runtime has no lineage for
// was planned against another serving generation.
func (r *Runtime) WaitLineageVisible(ctx context.Context, ticks map[string]uint64) error {
	if r == nil {
		return merr.WrapErrServiceInternalMsg("growing runtime is nil")
	}
	if len(ticks) == 0 {
		return ctx.Err()
	}
	return r.waitUntil(ctx, func() (bool, error) {
		l := r.lineage
		if l == nil {
			return false, viewerror.NewViewInvalidated("plan carries lineage ticks but the vchannel has no split lineage on this streaming node")
		}
		for vchannel := range ticks {
			if _, ok := l.applied[vchannel]; !ok {
				return false, viewerror.NewViewInvalidated("plan lineage vchannel %s is not a target of this split source", vchannel)
			}
		}
		if l.done {
			return true, nil
		}
		if l.err != nil {
			return false, l.err
		}
		for vchannel, tick := range ticks {
			if l.applied[vchannel] < tick {
				return false, nil
			}
		}
		return true, nil
	})
}

// waitUntil waits on mvccCond until ready reports true or an error.
func (r *Runtime) waitUntil(ctx context.Context, ready func() (bool, error)) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.mvccCond.Broadcast()
			r.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		ok, err := ready()
		if err != nil {
			return err
		}
		if ok {
			return ctx.Err()
		}
		if r.closed {
			return viewerror.NewViewInvalidated("growing runtime is closed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mvccCond.Wait()
	}
}

// Queue memory is node-accounted; overflow ends the attempt without advancing
// the runtime's applied cursor. Its next attempt rereads authoritative Summary.
const lineageQueueBytes int64 = 4 << 20

type (
	lineageQueuedEvent struct {
		event wal.TransformLogStreamEvent
		bytes int64
	}
	lineageHandler struct {
		ctx       context.Context
		events    chan lineageQueuedEvent
		done      chan struct{}
		closeOnce sync.Once
		mu        sync.Mutex
		bytes     int64
		err       error
	}
)

func newLineageHandler(ctx context.Context) *lineageHandler {
	return &lineageHandler{ctx: ctx, events: make(chan lineageQueuedEvent, 16), done: make(chan struct{})}
}

func (h *lineageHandler) Handle(event wal.TransformLogStreamEvent) {
	bytes := int64(64) + wal.TransformEntryMemory(event.Entry)
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-h.done:
		return
	case <-h.ctx.Done():
		return
	default:
	}
	if bytes > lineageQueueBytes-h.bytes || !wal.TransformMemory.TryReserveBuffered(bytes) {
		h.err = &lineageQueueOverflow{through: lineageEventTick(event)}
		h.closeOnce.Do(func() { close(h.done) })
		return
	}
	select {
	case h.events <- lineageQueuedEvent{event: event, bytes: bytes}:
		h.bytes += bytes
	default:
		wal.TransformMemory.Release(bytes)
		h.err = &lineageQueueOverflow{through: lineageEventTick(event)}
		h.closeOnce.Do(func() { close(h.done) })
	}
}

func (h *lineageHandler) release(bytes int64) {
	h.mu.Lock()
	h.bytes -= bytes
	h.mu.Unlock()
	wal.TransformMemory.Release(bytes)
}
func (h *lineageHandler) Error() error { h.mu.Lock(); defer h.mu.Unlock(); return h.err }
func (h *lineageHandler) Close()       { h.closeOnce.Do(func() { close(h.done) }) }
func (h *lineageHandler) abort() {
	h.Close()
	for {
		select {
		case queued := <-h.events:
			h.release(queued.bytes)
		default:
			return
		}
	}
}

// replayLineageInto rebuilds only the staged partitions, one bounded Summary
// page at a time, rather than retaining every target delete since the fence.
// It first catches a fixed prefix without blocking the serving residual, then
// locks application to replay the finite tail and atomically publish the merge.
func (r *Runtime) replayLineageInto(ctx context.Context, segments []*growingSegment) (func(), error) {
	r.mu.RLock()
	l := r.lineage
	streams := r.lineageStreams
	r.mu.RUnlock()
	if l == nil || len(segments) == 0 {
		return func() {}, nil
	}
	l.applyMu.Lock()
	l.replaySegments = segments
	r.mu.RLock()
	targets := make(map[string]uint64, len(l.applied))
	for channel, tick := range l.applied {
		targets[channel] = tick
	}
	r.mu.RUnlock()
	l.applyMu.Unlock()
	// New target entries already apply to staged segments. Replaying the captured
	// old prefix can run out of timestamp order; native deletes are idempotent
	// and preserve earliest effective deletion. No query can see the stage yet.
	for channel, through := range targets {
		cursor := l.spec.switchTimeTick
		for cursor < through {
			after, err := wal.ReplayTransformPage(ctx, streams, channel, cursor, through, func(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
				return r.applyTransformLogEntryTo(ctx, entry, segments)
			})
			if err != nil {
				l.applyMu.Lock()
				l.replaySegments = nil
				l.applyMu.Unlock()
				return nil, err
			}
			cursor = after
		}
	}
	l.applyMu.Lock()
	l.replaySegments = nil
	return l.applyMu.Unlock, nil
}

// restartLineageIfResidualReturnedLocked starts a new subscription from
// T_switch when a partition refresh brought residual segments into a runtime
// whose subscription had already ended. Caller holds r.mu.
func (r *Runtime) restartLineageIfResidualReturnedLocked() {
	if r.lineageSpec == nil || r.lineage == nil || !r.lineage.done || len(r.segments) == 0 {
		return
	}
	r.startLineageLocked()
}

// Internal overflow signal carries the missed ordered prefix, not its payload.
type lineageQueueOverflow struct{ through uint64 }

func (e *lineageQueueOverflow) Error() string {
	return "residual transform queue overflow; replay from applied cursor"
}
func (e *lineageQueueOverflow) Unwrap() error { return merr.ErrServiceResourceInsufficient }
func lineageEventTick(event wal.TransformLogStreamEvent) uint64 {
	if event.Entry != nil {
		return event.Entry.GetTimeTick()
	}
	if event.SyncUp != nil {
		return event.SyncUp.TimeTick
	}
	return 0
}
