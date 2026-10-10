package transformlogbuffer

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Buffer struct {
	streams wal.TransformLogStreamManager

	mu                sync.Mutex
	streamsByPChannel map[string]*streamState
	channels          map[string]*vchannelBuffer

	catchupConcurrency int
	drainQueues        map[string]*drainQueue
}

func newBuffer(streams wal.TransformLogStreamManager, catchupConcurrency int) *Buffer {
	if catchupConcurrency <= 0 {
		panic("query view transform log drain concurrency must be positive")
	}
	b := &Buffer{
		streams:            streams,
		streamsByPChannel:  make(map[string]*streamState),
		channels:           make(map[string]*vchannelBuffer),
		catchupConcurrency: catchupConcurrency,
		drainQueues:        make(map[string]*drainQueue),
	}
	return b
}

func (b *Buffer) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	if view == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	meta := view.IntoProto().GetMeta()
	vchannel := meta.GetVchannel()
	startFrom := meta.GetTransformStartAfterTimetick()
	if vchannel == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	b.mu.Lock()
	buf := b.channels[vchannel]
	if buf == nil {
		stream, err := b.getOrCreateStreamLocked(ctx, pchannel)
		if err != nil {
			b.mu.Unlock()
			return nil, err
		}
		buf = newVChannelBuffer(b, pchannel, vchannel, startFrom)
		buf.stream = stream
		b.channels[vchannel] = buf
		stream.refs[vchannel] = buf
	}
	if err := buf.acquireLocked(startFrom); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	stream := buf.stream
	b.mu.Unlock()
	if stream != nil {
		if err := buf.ensureSubscribed(ctx, stream.stream); err != nil {
			buf.releaseGuard(startFrom)
			return nil, err
		}
	}
	return &guard{buffer: buf, startFrom: startFrom}, nil
}

func (b *Buffer) RegisterSegment(ctx context.Context, segment qnview.TransformSegment) (qnview.TransformRegistration, error) {
	if segment == nil || segment.VChannel() == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	b.mu.Lock()
	buf := b.channels[segment.VChannel()]
	b.mu.Unlock()
	if buf == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("transform log buffer for vchannel %q is not acquired", segment.VChannel())
	}
	return buf.registerSegment(ctx, segment)
}

type catchupTask struct {
	ctx        context.Context
	reg        *registration
	onComplete func(error)

	// Buffer.mu protects queue membership. Removing the element transfers
	// sole completion ownership to either a worker or a cancellation callback.
	element          *list.Element
	stopCancellation func()
}

// drainQueue is shared by all VChannels and logical stream generations of a
// PChannel. Buffer.mu protects its tasks and worker count.
type drainQueue struct {
	tasks   list.List
	workers int
}

func (b *Buffer) scheduleDrain(task *catchupTask) {
	pchannel := task.reg.buffer.pchannel
	b.mu.Lock()
	defer b.mu.Unlock()
	queue := b.drainQueues[pchannel]
	if queue == nil {
		queue = &drainQueue{}
		b.drainQueues[pchannel] = queue
	}
	// Submission must not occupy a physical-loading worker waiting for replay.
	task.element = queue.tasks.PushBack(task)
	stopTask := context.AfterFunc(task.ctx, func() { b.cancelDrain(queue, task, task.ctx.Err()) })
	stopRegistration := context.AfterFunc(task.reg.ctx, func() {
		b.cancelDrain(queue, task, context.Cause(task.reg.ctx))
	})
	task.stopCancellation = func() { stopTask(); stopRegistration() }
	b.startDrainWorkersLocked(pchannel, queue)
}

// startDrainWorkersLocked immediately uses newly available capacity, including
// when a config update grows the limit without any new task submissions.
func (b *Buffer) startDrainWorkersLocked(pchannel string, queue *drainQueue) {
	for n := min(b.catchupConcurrency-queue.workers, queue.tasks.Len()); n > 0; n-- {
		queue.workers++
		go b.drainWorker(pchannel, queue)
	}
}

// takeDrainLocked is the only transition out of the pending queue. After it
// succeeds, cancellation cannot complete the task ahead of an in-flight Apply.
func (b *Buffer) takeDrainLocked(queue *drainQueue, task *catchupTask) bool {
	if task.element == nil {
		return false
	}
	queue.tasks.Remove(task.element)
	task.element = nil
	task.stopCancellation()
	return true
}

func (b *Buffer) cancelDrain(queue *drainQueue, task *catchupTask, err error) {
	b.mu.Lock()
	owned := b.takeDrainLocked(queue, task)
	b.mu.Unlock()
	if owned {
		// AfterFunc runs asynchronously: Unregister/Release must never join
		// a completion callback which can re-enter the manager or shard.
		task.complete(err)
	}
}

func (task *catchupTask) complete(err error) {
	if err != nil {
		task.reg.Unregister()
	}
	task.onComplete(err)
}

func (b *Buffer) drainWorker(pchannel string, queue *drainQueue) {
	for {
		b.mu.Lock()
		front := queue.tasks.Front()
		if front == nil || queue.workers > b.catchupConcurrency {
			queue.workers--
			if queue.workers == 0 {
				delete(b.drainQueues, pchannel)
			}
			b.mu.Unlock()
			return
		}
		task := front.Value.(*catchupTask)
		b.takeDrainLocked(queue, task)
		b.mu.Unlock()
		task.complete(task.reg.buffer.drainRegistration(task.ctx, task.reg))
	}
}

func (b *Buffer) getOrCreateStreamLocked(ctx context.Context, pchannel string) (*streamState, error) {
	if state := b.streamsByPChannel[pchannel]; state != nil {
		select {
		case <-state.stream.Done():
			// This is the logical stream's terminal Done, not a physical RPC
			// disconnect. Old buffers keep their own state until released.
			delete(b.streamsByPChannel, pchannel)
			if len(state.refs) == 0 {
				state.close()
			}
		default:
			return state, nil
		}
	}
	if b.streams == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	// The buffer owns the shared stream; a view only owns its acquisition.
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	stream, err := b.streams.AcquireStream(streamCtx, pchannel)
	stop()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		cancel()
		_ = stream.Close()
		return nil, err
	}
	mlog.Debug(ctx, "querynode transform log buffer acquired pchannel stream",
		mlog.FieldPChannel(pchannel),
	)
	state := &streamState{
		pchannel: pchannel,
		stream:   stream,
		cancel:   cancel,
		refs:     make(map[string]*vchannelBuffer),
	}
	b.streamsByPChannel[pchannel] = state
	return state, nil
}

func (b *Buffer) removeLocked(vchannel string, buf *vchannelBuffer) *streamState {
	if b.channels[vchannel] == buf {
		delete(b.channels, vchannel)
	}
	if state := buf.stream; state != nil {
		delete(state.refs, vchannel)
		if len(state.refs) == 0 {
			if b.streamsByPChannel[buf.pchannel] == state {
				delete(b.streamsByPChannel, buf.pchannel)
			}
			return state
		}
	}
	return nil
}

type streamState struct {
	pchannel string
	stream   wal.TransformLogStream
	cancel   context.CancelFunc
	refs     map[string]*vchannelBuffer
}

func (s *streamState) close() {
	s.cancel()
	_ = s.stream.Close()
}

type bufEventHandler struct {
	buffer *vchannelBuffer
}

func (h bufEventHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received subscription error",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Err(event.Err),
		)
		h.buffer.fail(event.Err)
		return nil
	}
	if event.Entry != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received entry",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.Entry.GetTimeTick()),
		)
		if err := h.buffer.onEntry(event.Entry); err != nil {
			return err
		}
	}
	if event.SyncUp != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received sync-up",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("timeTick", event.SyncUp.TimeTick),
		)
		return h.buffer.onSyncUp(event.SyncUp.TimeTick)
	}
	return nil
}

func (h bufEventHandler) Close() {}

type guard struct {
	once      sync.Once
	buffer    *vchannelBuffer
	startFrom uint64
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
	stream   *streamState
	sub      wal.TransformLogSubscription

	subscribeAttempt *subscribeAttempt

	mu               sync.Mutex
	retentionStart   uint64
	visibleTimeTick  uint64
	visibilityNotify chan struct{}
	guards           map[uint64]int
	entries          []*streamingpb.TransformLogEntry
	live             map[int64]*registration
	pending          map[int64]*registration
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
		live:             make(map[int64]*registration),
		pending:          make(map[int64]*registration),
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
		Handler:            bufEventHandler{buffer: b},
	})
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
		return merr.WrapErrServiceUnavailableMsg("transform log buffer range starts from %d, cannot serve %d", b.retentionStart, startFrom)
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
		return nil, merr.WrapErrServiceUnavailableMsg("transform log buffer range starts from %d, cannot serve segment %d from %d", b.retentionStart, segment.ID(), startFrom)
	}
	reg := newRegistration(b, segment)
	b.pending[segment.ID()] = reg
	mlog.Debug(ctx, "querynode transform log buffer registered segment",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.FieldSegmentID(segment.ID()),
		mlog.Uint64("startAfterTimeTick", startFrom),
		mlog.Int("pendingSegments", len(b.pending)),
		mlog.Int("liveSegments", len(b.live)),
	)
	b.mu.Unlock()

	return reg, nil
}

func (b *vchannelBuffer) drainRegistration(ctx context.Context, reg *registration) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, done, notify, err := b.nextCatchupBatch(reg)
		if err != nil || done {
			return err
		}
		if len(batch) == 0 {
			select {
			case <-notify:
				continue
			case <-reg.ctx.Done():
				return context.Cause(reg.ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for _, entry := range batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			mlog.Debug(ctx, "querynode transform log buffer drains entry to segment",
				mlog.FieldPChannel(b.pchannel),
				mlog.FieldVChannel(b.vchannel),
				mlog.FieldSegmentID(reg.segment.ID()),
				mlog.Uint64("timeTick", entry.GetTimeTick()),
			)
			if err := reg.applyEntry(entry); err != nil {
				return err
			}
			if entry.GetTimeTick() > reg.drainedTo.Load() {
				reg.drainedTo.Store(entry.GetTimeTick())
			}
		}
	}
}

func (b *vchannelBuffer) nextCatchupBatch(reg *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return nil, false, nil, b.err
	}
	if err := context.Cause(reg.ctx); err != nil {
		return nil, false, nil, err
	}
	if b.pending[reg.segment.ID()] != reg {
		return nil, false, nil, context.Canceled
	}
	batch := make([]*streamingpb.TransformLogEntry, 0)
	for _, entry := range b.entries {
		if entry.GetTimeTick() > reg.drainedTo.Load() {
			batch = append(batch, entry)
		}
	}
	if len(batch) == 0 {
		if !b.syncUp {
			return nil, false, b.visibilityNotify, nil
		}
		delete(b.pending, reg.segment.ID())
		b.live[reg.segment.ID()] = reg
		b.trimLocked()
		return nil, true, nil, nil
	}
	return batch, false, nil, nil
}

func (b *vchannelBuffer) waitTransformVisible(ctx context.Context, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	waitLogged := false
	for {
		if b.err != nil {
			return b.err
		}
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending[reg.segment.ID()] == reg {
		delete(b.pending, reg.segment.ID())
	}
	if b.live[reg.segment.ID()] == reg {
		delete(b.live, reg.segment.ID())
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
			stream.close()
		}
		return
	}
	b.trimLocked()
	b.mu.Unlock()
	b.owner.mu.Unlock()
}

func (b *vchannelBuffer) trimLocked() {
	// View guards retain history for future loads; pending registrations also
	// retain their replay range until catch-up completes or they are removed.
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
	clear(b.entries[len(kept):])
	b.entries = kept
	b.retentionStart = minStart
}

func (b *vchannelBuffer) onEntry(entry *streamingpb.TransformLogEntry) error {
	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
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
		// A failed segment publishes Poison before this shared frontier advances.
		// Unregistered instances may return cancellation; other segments continue.
		if err := reg.applyEntry(entry); err != nil && reg.ctx.Err() == nil {
			b.fail(err)
			return err
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	if entry.GetTimeTick() > b.visibleTimeTick {
		b.visibleTimeTick = entry.GetTimeTick()
		mlog.Debug(context.TODO(), "querynode transform log buffer advanced visible timetick",
			mlog.FieldPChannel(b.pchannel),
			mlog.FieldVChannel(b.vchannel),
			mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
		)
		b.notifyVisibilityLocked()
	}
	return nil
}

func (b *vchannelBuffer) onSyncUp(timeTick uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
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
	b.notifyVisibilityLocked()
	return nil
}

func (b *vchannelBuffer) fail(err error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return
	}
	b.err = err
	// Pending registrations include both queued and running catch-up tasks.
	// Queued tasks can now complete independently of replay capacity; running
	// tasks keep their worker until Apply has returned.
	for _, reg := range b.pending {
		reg.cancel(err)
	}
	b.notifyVisibilityLocked()
	b.mu.Unlock()
}

func (b *vchannelBuffer) notifyVisibilityLocked() {
	close(b.visibilityNotify)
	b.visibilityNotify = make(chan struct{})
}

type registration struct {
	buffer    *vchannelBuffer
	segment   qnview.TransformSegment
	startFrom uint64
	drainedTo atomic.Uint64
	ctx       context.Context
	cancel    context.CancelCauseFunc
	applyMu   sync.Mutex
	poisoned  bool
	once      sync.Once
}

func newRegistration(buffer *vchannelBuffer, segment qnview.TransformSegment) *registration {
	ctx, cancel := context.WithCancelCause(context.Background()) //nolint:gosec // registration owns cancellation through Unregister
	reg := &registration{
		buffer:    buffer,
		segment:   segment,
		startFrom: segment.TransformStartAfterTimeTick(),
		ctx:       ctx,
		cancel:    cancel,
	}
	reg.drainedTo.Store(reg.startFrom)
	return reg
}

func (r *registration) Catchup(ctx context.Context, onComplete func(error)) {
	if err := ctx.Err(); err != nil {
		r.Unregister()
		onComplete(err)
		return
	}
	if err := context.Cause(r.ctx); err != nil {
		r.Unregister()
		onComplete(err)
		return
	}
	r.buffer.owner.scheduleDrain(&catchupTask{ctx: ctx, reg: r, onComplete: onComplete})
}

func (r *registration) Unregister() {
	r.once.Do(func() {
		r.cancel(nil)
		r.buffer.unregister(r)
		// Cancellation cannot interrupt an already running native Delete.
		// Wait without the buffer lock before allowing the owner to free it.
		r.applyMu.Lock()
		r.applyMu.Unlock() //nolint:staticcheck // synchronization barrier: wait for native Apply before release
	})
}

func (r *registration) applyEntry(entry *streamingpb.TransformLogEntry) error {
	r.applyMu.Lock()
	defer r.applyMu.Unlock()
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	if r.poisoned {
		return nil
	}
	if err := r.segment.ApplyTransform(r.ctx, entry); err != nil {
		r.poisoned = true
		if observer, ok := r.segment.(qnview.TransformFailureObserver); ok {
			observer.OnTransformFailed(entry.GetTimeTick(), err)
		} else {
			// Legacy consumers without a Poison observer must fail closed.
			return err
		}
		mlog.Warn(r.ctx, "segment poisoned after ApplyTransform failure",
			mlog.FieldSegmentID(r.segment.ID()), mlog.Uint64("timeTick", entry.GetTimeTick()), mlog.Err(err))
	}
	return nil
}
