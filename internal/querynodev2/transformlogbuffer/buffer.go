package transformlogbuffer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Buffer struct {
	streamMu sync.Mutex
	streams  wal.TransformLogStreamManager

	mu                sync.Mutex
	streamsByPChannel map[string]*streamState
	channels          map[string]*vchannelBuffer

	drainMu          sync.Mutex
	drainReady       *sync.Cond
	drainTasks       []*registration
	drainWorkers     int
	drainConcurrency int
	drainClosed      bool
	drainWG          sync.WaitGroup
}

func New(streams wal.TransformLogStreamManager, drainConcurrency int) *Buffer {
	if drainConcurrency <= 0 {
		panic("query view transform log drain concurrency must be positive")
	}
	b := &Buffer{
		streams:           streams,
		streamsByPChannel: make(map[string]*streamState),
		channels:          make(map[string]*vchannelBuffer),
	}
	b.drainReady = sync.NewCond(&b.drainMu)
	b.drainMu.Lock()
	b.resizeDrainLocked(drainConcurrency)
	b.drainMu.Unlock()
	return b
}

// NewConfigured creates the node-wide catchup pool, watching config until the
// owning QueryNode context ends. Cancellation retires workers after their page.
func NewConfigured(ctx context.Context, streams wal.TransformLogStreamManager) *Buffer {
	params := paramtable.Get()
	b := New(streams, params.QueryNodeCfg.QueryViewTransformLogDrainConcurrency.GetAsInt())
	refresh := func(*config.Event) {
		b.drainMu.Lock()
		defer b.drainMu.Unlock()
		if !b.drainClosed {
			b.resizeDrainLocked(params.QueryNodeCfg.QueryViewTransformLogDrainConcurrency.GetAsInt())
		}
	}
	watcher := config.NewHandler(fmt.Sprintf("query-transform-drain-%p", b), refresh)
	params.Watch(params.QueryNodeCfg.QueryViewTransformLogDrainConcurrency.Key, watcher)
	refresh(nil)
	context.AfterFunc(ctx, func() {
		params.Unwatch(params.QueryNodeCfg.QueryViewTransformLogDrainConcurrency.Key, watcher)
		b.closeDrain()
	})
	return b
}

func (b *Buffer) resizeDrainLocked(concurrency int) {
	b.drainConcurrency = concurrency
	for b.drainWorkers < concurrency {
		b.drainWorkers++
		b.drainWG.Add(1)
		go b.drainWorker()
	}
	b.drainReady.Broadcast()
}

func (b *Buffer) closeDrain() {
	b.drainMu.Lock()
	b.drainClosed = true
	b.drainTasks = nil
	b.drainReady.Broadcast()
	b.drainMu.Unlock()
	b.drainWG.Wait()
}

// Bind records a range reference without opening a stream or doing I/O.
func (b *Buffer) Bind(view *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	if view == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	meta := view.IntoProto().GetMeta()
	channel, start := meta.GetVchannel(), meta.GetTransformStartAfterTimetick()
	if channel == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	buf := b.channels[channel]
	if buf == nil {
		buf = newVChannelBuffer(b, funcutil.ToPhysicalChannel(channel), channel, start)
		b.channels[channel] = buf
	}
	if err := buf.acquireLocked(start); err != nil {
		return nil, err
	}
	return &guard{buffer: buf, startFrom: start}, nil
}

func (b *Buffer) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	g, err := b.Bind(view)
	if err != nil {
		return nil, err
	}
	if _, err := g.Prepare(ctx); err != nil {
		g.Release()
		return nil, err
	}
	return g, nil
}

func (b *Buffer) RegisterSegment(ctx context.Context, segment qnview.TransformSegment) (qnview.TransformRegistration, error) {
	if segment == nil || segment.VChannel() == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	b.mu.Lock()
	buf := b.channels[segment.VChannel()]
	b.mu.Unlock()
	if buf == nil {
		return nil, merr.WrapErrServiceInternalMsg("transform log buffer for vchannel %q is not acquired", segment.VChannel())
	}
	return buf.registerSegment(ctx, segment)
}

// scheduleDrainLocked coalesces runnable registrations. Caller holds the
// VChannel lock; a worker never holds drainMu while acquiring that lock.
// The queue contains at most one task per registration and never blocks dispatch.
func (b *vchannelBuffer) scheduleDrainLocked(reg *registration) {
	if reg.scheduled || b.pending[reg] != reg || b.err != nil || reg.ctx.Err() != nil {
		return
	}
	reg.scheduled = true
	owner := b.owner
	owner.drainMu.Lock()
	if owner.drainClosed {
		reg.scheduled = false
		owner.drainMu.Unlock()
		return
	}
	owner.drainTasks = append(owner.drainTasks, reg)
	owner.drainReady.Signal()
	owner.drainMu.Unlock()
}

func (b *Buffer) drainWorker() {
	defer b.drainWG.Done()
	for {
		b.drainMu.Lock()
		for len(b.drainTasks) == 0 && !b.drainClosed && b.drainWorkers <= b.drainConcurrency {
			b.drainReady.Wait()
		}
		if b.drainClosed || b.drainWorkers > b.drainConcurrency {
			b.drainWorkers--
			b.drainMu.Unlock()
			return
		}
		reg := b.drainTasks[0]
		b.drainTasks[0] = nil
		b.drainTasks = b.drainTasks[1:]
		b.drainMu.Unlock()
		more, done, err := reg.buffer.drainRegistration(reg.ctx, reg)
		reg.buffer.mu.Lock()
		// A parked pass already cleared its bit under the producer lock. An
		// arrival may now have scheduled a successor; do not clear that bit.
		if more || done || err != nil {
			reg.scheduled = false
		}
		if err != nil {
			delete(reg.buffer.pending, reg)
			reg.buffer.trimLocked()
		} else if more {
			reg.buffer.scheduleDrainLocked(reg)
		}
		reg.buffer.mu.Unlock()
		if err != nil || done {
			reg.finish(err)
		}
	}
}

func (b *Buffer) getOrCreateStreamLocked(ctx context.Context, pchannel string) (*streamState, error) {
	if state := b.streamsByPChannel[pchannel]; state != nil {
		select {
		case <-state.stream.Done():
			if len(state.refs) == 0 {
				delete(b.streamsByPChannel, pchannel)
				b.mu.Unlock()
				_ = state.stream.Close()
				b.mu.Lock()
			} else {
				return state, nil
			}
		default:
			return state, nil
		}
	}
	if b.streams == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	b.mu.Unlock()
	stream, err := b.streams.AcquireStream(ctx, pchannel)
	b.mu.Lock()
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "querynode transform log buffer acquired pchannel stream",
		mlog.FieldPChannel(pchannel),
	)
	state := &streamState{
		pchannel: pchannel,
		stream:   stream,
		refs:     make(map[string]*vchannelBuffer),
	}
	b.streamsByPChannel[pchannel] = state
	return state, nil
}

func (b *Buffer) removeLocked(vchannel string, buf *vchannelBuffer) wal.TransformLogStream {
	if b.channels[vchannel] == buf {
		delete(b.channels, vchannel)
	}
	if state := b.streamsByPChannel[buf.pchannel]; state != nil {
		delete(state.refs, vchannel)
		if len(state.refs) == 0 {
			delete(b.streamsByPChannel, buf.pchannel)
			return state.stream
		}
	}
	return nil
}

type streamState struct {
	pchannel string
	stream   wal.TransformLogStream
	refs     map[string]*vchannelBuffer
}

type bufEventHandler struct {
	buffer *vchannelBuffer
	stream wal.TransformLogStream
}

func (h bufEventHandler) Handle(event wal.TransformLogStreamEvent) {
	if event.Entry != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received entry",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.Entry.GetTimeTick()),
		)
		h.buffer.onEntry(event.Entry)
	}
	if event.SyncUp != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received sync-up",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.SyncUp.TimeTick),
		)
		h.buffer.onSyncUp(event.SyncUp.TimeTick)
	}
}

func (h bufEventHandler) Close() {
	// Logical termination publishes its error before closing handlers. Physical
	// reconnects preserve this handler; canceling one subscription does not set
	// a stream error.
	if err := h.stream.Error(); err != nil {
		h.buffer.fail(err)
	}
}

type guard struct {
	once      sync.Once
	buffer    *vchannelBuffer
	startFrom uint64
}

func (g *guard) Prepare(ctx context.Context) (bool, error) {
	b := g.buffer.owner
	b.streamMu.Lock()
	b.mu.Lock()
	stream, err := b.getOrCreateStreamLocked(ctx, g.buffer.pchannel)
	if err == nil {
		stream.refs[g.buffer.vchannel] = g.buffer
	}
	b.mu.Unlock()
	b.streamMu.Unlock()
	if err != nil {
		return true, err
	}
	// A later reference must observe terminal failure even without a subscription.
	if err := stream.stream.Error(); err != nil {
		g.buffer.fail(err)
		return false, err
	}
	err = g.buffer.ensureSubscribed(ctx, stream.stream)
	// An already terminated stream can reject Subscribe without creating a
	// subscription or invoking its handler Close callback.
	if terminalErr := stream.stream.Error(); terminalErr != nil {
		g.buffer.fail(terminalErr)
	}
	g.buffer.mu.Lock()
	if g.buffer.err != nil {
		err = g.buffer.err
	}
	retryable := g.buffer.err == nil
	g.buffer.mu.Unlock()
	return retryable, err
}

func (g *guard) Release() {
	g.once.Do(func() {
		g.buffer.releaseGuard(g.startFrom)
	})
}

func (g *guard) WaitTransformVisible(ctx context.Context, timetick uint64) error {
	return g.buffer.waitTransformVisible(ctx, timetick)
}

type vchannelBuffer struct {
	owner    *Buffer
	pchannel string
	vchannel string
	sub      wal.TransformLogSubscription

	subscribeAttempt *subscribeAttempt

	mu               sync.Mutex
	retentionStart   uint64
	visibleTimeTick  uint64
	visibilityNotify chan struct{}
	guards           map[uint64]int
	entries          []*streamingpb.TransformLogEntry
	live             map[*registration]*registration
	pending          map[*registration]*registration
	syncUp           bool
	err              error
}

func newVChannelBuffer(owner *Buffer, pchannel string, vchannel string, startFrom uint64) *vchannelBuffer {
	return &vchannelBuffer{
		owner:            owner,
		pchannel:         pchannel,
		vchannel:         vchannel,
		retentionStart:   startFrom,
		visibleTimeTick:  startFrom,
		visibilityNotify: make(chan struct{}),
		guards:           make(map[uint64]int),
		live:             make(map[*registration]*registration),
		pending:          make(map[*registration]*registration),
	}
}

type subscribeAttempt struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

func (b *vchannelBuffer) ensureSubscribed(ctx context.Context, stream wal.TransformLogStream) error {
	b.mu.Lock()
	if b.sub != nil {
		b.mu.Unlock()
		return nil
	}
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
	if b.subscribeAttempt != nil {
		attempt := b.subscribeAttempt
		b.mu.Unlock()
		return b.waitSubscribe(ctx, attempt)
	}

	// The VChannel's references own this work; each caller only owns its wait.
	subscribeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // canceled on attempt completion or final guard release
	attempt := &subscribeAttempt{done: make(chan struct{}), cancel: cancel}
	b.subscribeAttempt = attempt
	startAfter := b.retentionStart
	b.mu.Unlock()
	go b.subscribe(subscribeCtx, stream, attempt, startAfter) //nolint:gosec // shared work must outlive individual request contexts
	return b.waitSubscribe(ctx, attempt)
}

func (b *vchannelBuffer) waitSubscribe(ctx context.Context, attempt *subscribeAttempt) error {
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *vchannelBuffer) subscribe(ctx context.Context, stream wal.TransformLogStream, attempt *subscribeAttempt, startAfter uint64) {
	defer attempt.cancel()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           b.vchannel,
		StartAfterTimeTick: startAfter,
		Handler:            bufEventHandler{buffer: b, stream: stream},
	})
	if streamErr := stream.Error(); streamErr != nil {
		b.fail(streamErr)
		if err != nil {
			err = streamErr
		}
	}
	if err != nil {
		if isUnrecoverableSubscribeError(err) {
			b.fail(err)
		}
		b.mu.Lock()
		b.completeSubscribeLocked(attempt, err)
		b.mu.Unlock()
		return
	}

	closeSub := false
	b.mu.Lock()
	err = b.err
	if len(b.guards) == 0 {
		// Subscribe can succeed concurrently with the final reference release.
		err = context.Canceled
	}
	if err != nil {
		closeSub = true
		b.completeSubscribeLocked(attempt, err)
	} else {
		b.sub = sub
		b.completeSubscribeLocked(attempt, nil)
	}
	b.mu.Unlock()
	if closeSub {
		_ = sub.Close()
		return
	}
	mlog.Debug(context.TODO(), "querynode transform log buffer subscribed vchannel",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.Uint64("startAfterTimeTick", startAfter),
		mlog.Int64("subscriptionID", sub.ID()),
	)
}

func (b *vchannelBuffer) completeSubscribeLocked(attempt *subscribeAttempt, err error) {
	if b.subscribeAttempt == attempt {
		attempt.err = err
		b.subscribeAttempt = nil
		close(attempt.done)
	}
}

func isUnrecoverableSubscribeError(err error) bool {
	return errors.Is(err, wal.ErrTransformLogInvalidReadOption) ||
		errors.Is(err, wal.ErrTransformLogVChannelUnavailable) ||
		errors.Is(err, wal.ErrTransformLogStartPointTruncated)
}

func (b *vchannelBuffer) acquireLocked(startFrom uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	if startFrom < b.retentionStart {
		return merr.WrapErrServiceInternalMsg("transform log buffer range starts from %d, cannot serve %d", b.retentionStart, startFrom)
	}
	b.guards[startFrom]++
	return nil
}

func (b *vchannelBuffer) registerSegment(ctx context.Context, segment qnview.TransformSegment) (qnview.TransformRegistration, error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return nil, b.err
	}
	startFrom := segment.TransformStartAfterTimeTick()
	if startFrom < b.retentionStart {
		b.mu.Unlock()
		return nil, merr.WrapErrServiceInternalMsg("transform log buffer range starts from %d, cannot serve segment %d from %d", b.retentionStart, segment.ID(), startFrom)
	}
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	reg := newRegistration(b, segment)
	b.pending[reg] = reg
	mlog.Debug(ctx, "querynode transform log buffer registered segment",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.FieldSegmentID(segment.ID()),
		mlog.Uint64("startAfterTimeTick", startFrom),
		mlog.Int("pendingSegments", len(b.pending)),
		mlog.Int("liveSegments", len(b.live)),
	)
	b.scheduleDrainLocked(reg)
	b.mu.Unlock()
	return reg, nil
}

// A pass applies a bounded page, then yields to other registrations. Waiting
// for transport coverage retains state, not a worker. Dispatch and the empty
// check/live attachment share b.mu, so no arriving entry can fall between them.
func (b *vchannelBuffer) drainRegistration(ctx context.Context, reg *registration) (more, done bool, err error) {
	b.mu.Lock()
	if b.err != nil {
		err = b.err
	} else if b.pending[reg] != reg || ctx.Err() != nil {
		err = context.Canceled
	}
	if err != nil {
		b.mu.Unlock()
		return false, false, err
	}
	if b.syncUp {
		reg.setCatchupFrontier(b.visibleTimeTick)
	}
	first := sort.Search(len(b.entries), func(i int) bool {
		return b.entries[i].GetTimeTick() > reg.drainedTo.Load()
	})
	end := min(first+256, len(b.entries))
	batch := append([]*streamingpb.TransformLogEntry(nil), b.entries[first:end]...)
	frontier := b.visibleTimeTick
	if end < len(b.entries) {
		frontier = batch[len(batch)-1].GetTimeTick()
	}
	if len(batch) == 0 {
		if !b.syncUp {
			// Clear before dropping the producer lock. An arrival now either
			// schedules a successor or was included in this pass's observation.
			reg.scheduled = false
			b.mu.Unlock()
			return false, false, nil
		}
		delete(b.pending, reg)
		b.live[reg] = reg
		b.trimLocked()
		reg.publishProgress(frontier)
		b.mu.Unlock()
		return false, true, nil
	}
	b.mu.Unlock()
	for _, entry := range batch {
		if err := reg.apply(ctx, entry); err != nil {
			return false, false, err
		}
	}
	reg.publishProgress(frontier)
	return true, false, nil
}

func (b *vchannelBuffer) waitTransformVisible(ctx context.Context, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	waitLogged := false
	for {
		if timetick <= b.retentionStart || b.visibleTimeTick >= timetick {
			if waitLogged {
				mlog.Debug(ctx, "querynode transform log buffer wait visible done",
					mlog.FieldPChannel(b.pchannel),
					mlog.FieldVChannel(b.vchannel),
					mlog.Uint64("targetTimeTick", timetick),
					mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
					mlog.Uint64("retentionStart", b.retentionStart),
				)
			}
			return nil
		}
		if b.err != nil {
			return b.err
		}
		if !waitLogged {
			waitLogged = true
			mlog.Debug(ctx, "querynode transform log buffer wait visible",
				mlog.FieldPChannel(b.pchannel),
				mlog.FieldVChannel(b.vchannel),
				mlog.Uint64("targetTimeTick", timetick),
				mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
				mlog.Uint64("retentionStart", b.retentionStart),
				mlog.Bool("syncUp", b.syncUp),
			)
		}
		notify := b.visibilityNotify
		b.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			b.mu.Lock()
			mlog.Debug(ctx, "querynode transform log buffer wait visible canceled",
				mlog.FieldPChannel(b.pchannel),
				mlog.FieldVChannel(b.vchannel),
				mlog.Uint64("targetTimeTick", timetick),
				mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
				mlog.Uint64("retentionStart", b.retentionStart),
				mlog.Bool("syncUp", b.syncUp),
				mlog.Err(ctx.Err()),
			)
			return ctx.Err()
		}
		b.mu.Lock()
	}
}

func (b *vchannelBuffer) unregister(reg *registration) {
	b.removeRegistration(reg)
}

func (b *vchannelBuffer) removeRegistration(reg *registration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pending, reg)
	if b.live[reg] == reg {
		delete(b.live, reg)
	}
	b.trimLocked()
}

func (b *vchannelBuffer) releaseGuard(startFrom uint64) {
	b.owner.mu.Lock()
	b.mu.Lock()
	if count := b.guards[startFrom]; count > 1 {
		b.guards[startFrom] = count - 1
		b.trimLocked()
		b.mu.Unlock()
		b.owner.mu.Unlock()
		return
	}
	delete(b.guards, startFrom)
	if len(b.guards) == 0 {
		sub := b.sub
		attempt := b.subscribeAttempt
		stream := b.owner.removeLocked(b.vchannel, b)
		b.mu.Unlock()
		b.owner.mu.Unlock()
		if attempt != nil {
			attempt.cancel()
		}
		if sub != nil {
			_ = sub.Close()
		}
		if stream != nil {
			_ = stream.Close()
		}
		return
	}
	b.trimLocked()
	b.mu.Unlock()
	b.owner.mu.Unlock()
}

func (b *vchannelBuffer) trimLocked() {
	minStart := uint64(0)
	first := true
	for startFrom := range b.guards {
		if first || startFrom < minStart {
			minStart = startFrom
			first = false
		}
	}
	for _, reg := range b.pending {
		if first || reg.startFrom < minStart {
			minStart = reg.startFrom
			first = false
		}
	}
	if first || minStart <= b.retentionStart {
		return
	}
	kept := b.entries[:0]
	for _, entry := range b.entries {
		if entry.GetTimeTick() > minStart {
			kept = append(kept, entry)
		}
	}
	// Release payloads in the unused backing-array tail as well. The frontier
	// above remains determined by retained Views and pending replay pins.
	clear(b.entries[len(kept):])
	b.entries = kept
	b.retentionStart = minStart
}

func (b *vchannelBuffer) onEntry(entry *streamingpb.TransformLogEntry) {
	b.mu.Lock()
	if entry.GetTimeTick() > b.retentionStart {
		b.entries = append(b.entries, entry)
	}
	applies := make([]*registration, 0, len(b.live))
	for _, reg := range b.live {
		applies = append(applies, reg)
	}
	b.mu.Unlock()

	for _, reg := range applies {
		mlog.Debug(context.TODO(), "querynode transform log buffer applies entry to live segment",
			mlog.FieldPChannel(b.pchannel),
			mlog.FieldVChannel(b.vchannel),
			mlog.FieldSegmentID(reg.segment.ID()),
			mlog.Uint64("timeTick", entry.GetTimeTick()),
		)
		if err := reg.apply(reg.ctx, entry); err != nil {
			// Apply has failed this concrete registration, not the shared log.
			// Other instances must still consume this entry and future entries.
			b.removeRegistration(reg)
		}
	}

	b.mu.Lock()
	if entry.GetTimeTick() > b.visibleTimeTick {
		b.visibleTimeTick = entry.GetTimeTick()
		mlog.Debug(context.TODO(), "querynode transform log buffer advanced visible timetick",
			mlog.FieldPChannel(b.pchannel),
			mlog.FieldVChannel(b.vchannel),
			mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
		)
		b.notifyVisibilityLocked()
	}
	for _, reg := range b.pending {
		b.scheduleDrainLocked(reg)
	}
	b.mu.Unlock()
}

func (b *vchannelBuffer) onSyncUp(timeTick uint64) {
	b.mu.Lock()
	if timeTick > b.visibleTimeTick {
		b.visibleTimeTick = timeTick
	}
	if !b.syncUp {
		b.syncUp = true
	}
	mlog.Debug(context.TODO(), "querynode transform log buffer marked sync-up",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
	)
	for _, reg := range b.live {
		reg.publishProgress(timeTick)
	}
	for _, reg := range b.pending {
		b.scheduleDrainLocked(reg)
	}
	b.notifyVisibilityLocked()
	b.mu.Unlock()
}

func (b *vchannelBuffer) fail(err error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return
	}
	b.err = err
	b.notifyVisibilityLocked()
	regs := make([]*registration, 0, len(b.live)+len(b.pending))
	for _, reg := range b.live {
		regs = append(regs, reg)
	}
	for _, reg := range b.pending {
		regs = append(regs, reg)
	}
	b.mu.Unlock()

	for _, reg := range regs {
		reg.finish(err)
	}
}

func (b *vchannelBuffer) notifyVisibilityLocked() {
	close(b.visibilityNotify)
	b.visibilityNotify = make(chan struct{})
}

type registration struct {
	scheduled       bool        // protected by buffer.mu; queued or executing catchup
	onFailure       func(error) // protected by errMu
	applyMu         sync.Mutex
	buffer          *vchannelBuffer
	segment         qnview.TransformSegment
	startFrom       uint64
	drainedTo       atomic.Uint64
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	err             error
	errMu           sync.Mutex
	once            sync.Once
	finishOnce      sync.Once
	catchupFrontier uint64
	hasFrontier     bool
	progressNotify  chan struct{}
}

func newRegistration(buffer *vchannelBuffer, segment qnview.TransformSegment) *registration {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel ownership transfers to registration.Unregister.
	reg := &registration{
		buffer:         buffer,
		segment:        segment,
		startFrom:      segment.TransformStartAfterTimeTick(),
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
		progressNotify: make(chan struct{}),
	}
	reg.drainedTo.Store(segment.TransformStartAfterTimeTick())
	return reg
}

func (r *registration) WaitCatchup(ctx context.Context) error {
	select {
	case <-r.done:
		r.errMu.Lock()
		defer r.errMu.Unlock()
		if r.err != nil {
			mlog.Debug(ctx, "querynode transform log buffer segment catchup failed",
				mlog.FieldPChannel(r.buffer.pchannel),
				mlog.FieldVChannel(r.buffer.vchannel),
				mlog.FieldSegmentID(r.segment.ID()),
				mlog.Uint64("startAfterTimeTick", r.startFrom),
				mlog.Uint64("drainedTo", r.drainedTo.Load()),
				mlog.Err(r.err),
			)
		}
		return r.err
	case <-ctx.Done():
		mlog.Debug(ctx, "querynode transform log buffer segment catchup canceled",
			mlog.FieldPChannel(r.buffer.pchannel),
			mlog.FieldVChannel(r.buffer.vchannel),
			mlog.FieldSegmentID(r.segment.ID()),
			mlog.Uint64("startAfterTimeTick", r.startFrom),
			mlog.Uint64("drainedTo", r.drainedTo.Load()),
			mlog.Err(ctx.Err()),
		)
		return ctx.Err()
	}
}

func (r *registration) Unregister() {
	r.once.Do(func() {
		r.cancel()
		r.buffer.unregister(r)
		// Drain/application may already hold a reference copied from the live set.
		r.applyMu.Lock()
		r.applyMu.Unlock() //nolint:staticcheck // Wait for the last in-flight application before freeing the physical segment.
		r.finish(context.Canceled)
	})
}

func (r *registration) finish(err error) {
	r.errMu.Lock()
	var callback func(error)
	if err != nil && r.err == nil {
		r.err = err
		callback = r.onFailure
		close(r.progressNotify)
		r.progressNotify = make(chan struct{})
	}
	r.finishOnce.Do(func() { close(r.done) })
	r.errMu.Unlock()
	if callback != nil {
		callback(err)
	}
}

func (r *registration) OnFailure(callback func(error)) {
	r.errMu.Lock()
	r.onFailure = callback
	err := r.err
	r.errMu.Unlock()
	if err != nil {
		callback(err)
	}
}

func (r *registration) setCatchupFrontier(tick uint64) {
	r.errMu.Lock()
	if !r.hasFrontier {
		r.catchupFrontier, r.hasFrontier = tick, true
	}
	if r.drainedTo.Load() >= r.catchupFrontier {
		r.finishOnce.Do(func() { close(r.done) })
	}
	r.errMu.Unlock()
}

func (r *registration) publishProgress(tick uint64) {
	r.errMu.Lock()
	if r.err != nil {
		r.errMu.Unlock()
		return
	}
	if tick > r.drainedTo.Load() {
		r.drainedTo.Store(tick)
		close(r.progressNotify)
		r.progressNotify = make(chan struct{})
	}
	if r.hasFrontier && r.drainedTo.Load() >= r.catchupFrontier {
		r.finishOnce.Do(func() { close(r.done) })
	}
	r.errMu.Unlock()
}

func (r *registration) WaitTransformVisible(ctx context.Context, tick uint64) error {
	for {
		r.errMu.Lock()
		err, applied, notify := r.err, r.drainedTo.Load(), r.progressNotify
		r.errMu.Unlock()
		if err != nil {
			return err
		}
		if applied >= tick {
			return nil
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
}

func (r *registration) apply(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return err
	}
	r.errMu.Lock()
	err := r.err
	r.errMu.Unlock()
	if err != nil {
		return err
	}
	if entry.GetTimeTick() <= r.drainedTo.Load() {
		return nil
	}
	if err := r.segment.ApplyTransform(ctx, entry); err != nil {
		// A partially applied entry cannot be retried on this physical base.
		// Publish failure before shared delivery advances; instance MVCC waits
		// then reject every timestamp, including previously applied history.
		// Release cancellation only retires this registration.
		if r.ctx.Err() == nil {
			r.finish(err)
		}
		return err
	}
	r.publishProgress(entry.GetTimeTick())
	return nil
}
