package transformlogbuffer

import (
	"context"
	"io"
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
	streamMu sync.Mutex
	streams  wal.TransformLogStreamManager

	mu                sync.Mutex
	streamsByPChannel map[string]*streamState
	channels          map[string]*vchannelBuffer

	drainTasks chan *registration
}

func New(streams wal.TransformLogStreamManager, drainConcurrency int) *Buffer {
	if drainConcurrency <= 0 {
		panic("query view transform log drain concurrency must be positive")
	}
	b := &Buffer{
		streams:           streams,
		streamsByPChannel: make(map[string]*streamState),
		channels:          make(map[string]*vchannelBuffer),
		drainTasks:        make(chan *registration, 1024),
	}
	for i := 0; i < drainConcurrency; i++ {
		go b.drainWorker()
	}
	return b
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

func (b *Buffer) scheduleDrain(ctx context.Context, reg *registration) error {
	select {
	case b.drainTasks <- reg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Buffer) drainWorker() {
	for reg := range b.drainTasks {
		err := reg.buffer.drainRegistration(reg.ctx, reg)
		if err != nil {
			reg.buffer.removeRegistration(reg)
		}
		reg.finish(err)
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
	go b.watchStream(state)
	return state, nil
}

// The resumable stream owns transient reconnects. Only its terminal result
// invalidates the buffers and their pending or live segment registrations.
func (b *Buffer) watchStream(state *streamState) {
	<-state.stream.Done()
	err := streamTerminalError(state.stream)
	b.mu.Lock()
	buffers := make([]*vchannelBuffer, 0, len(state.refs))
	for _, buf := range state.refs {
		buffers = append(buffers, buf)
	}
	b.mu.Unlock()
	for _, buf := range buffers {
		buf.fail(err)
	}
}

func streamTerminalError(stream wal.TransformLogStream) error {
	select {
	case <-stream.Done():
		if err := stream.Error(); err != nil {
			return err
		}
		return io.EOF
	default:
		return nil
	}
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

func (h bufEventHandler) Close() {}

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
	err = g.buffer.ensureSubscribed(ctx, stream.stream)
	// A buffer can attach after the stream watcher has taken its snapshot, or
	// Subscribe can finish concurrently with stream termination.
	if terminalErr := streamTerminalError(stream.stream); terminalErr != nil {
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
	done chan struct{}
	err  error
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

	attempt := &subscribeAttempt{done: make(chan struct{})}
	b.subscribeAttempt = attempt
	startAfter := b.retentionStart
	b.mu.Unlock()
	return b.subscribe(ctx, stream, attempt, startAfter)
}

func (b *vchannelBuffer) waitSubscribe(ctx context.Context, attempt *subscribeAttempt) error {
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *vchannelBuffer) subscribe(ctx context.Context, stream wal.TransformLogStream, attempt *subscribeAttempt, startAfter uint64) error {
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
		return err
	}

	closeSub := false
	b.mu.Lock()
	if b.err != nil {
		err = b.err
		closeSub = true
		b.completeSubscribeLocked(attempt, err)
	} else {
		b.sub = sub
		b.completeSubscribeLocked(attempt, nil)
	}
	b.mu.Unlock()
	if closeSub {
		_ = sub.Close()
		return err
	}
	mlog.Debug(context.TODO(), "querynode transform log buffer subscribed vchannel",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.Uint64("startAfterTimeTick", startAfter),
		mlog.Int64("subscriptionID", sub.ID()),
	)
	return nil
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
	b.mu.Unlock()

	if err := b.owner.scheduleDrain(ctx, reg); err != nil {
		b.removeRegistration(reg)
		reg.finish(err)
		return nil, err
	}
	return reg, nil
}

func (b *vchannelBuffer) drainRegistration(ctx context.Context, reg *registration) error {
	for {
		batch, frontier, done, notify, err := b.nextCatchupBatch(reg)
		if err != nil || done {
			return err
		}
		if len(batch) == 0 {
			select {
			case <-notify:
				continue
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
			if err := reg.apply(ctx, entry); err != nil {
				return err
			}
		}
		// Receipt through an empty interval is progress only after this batch
		// has been applied to the concrete physical instance.
		reg.publishProgress(frontier)
	}
}

func (b *vchannelBuffer) nextCatchupBatch(reg *registration) ([]*streamingpb.TransformLogEntry, uint64, bool, <-chan struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return nil, 0, false, nil, b.err
	}
	if b.pending[reg] != reg {
		return nil, 0, false, nil, context.Canceled
	}
	if b.syncUp {
		reg.setCatchupFrontier(b.visibleTimeTick)
	}
	batch := make([]*streamingpb.TransformLogEntry, 0)
	for _, entry := range b.entries {
		if entry.GetTimeTick() > reg.drainedTo.Load() {
			batch = append(batch, entry)
		}
	}
	if len(batch) == 0 {
		if !b.syncUp {
			return nil, 0, false, b.visibilityNotify, nil
		}
		delete(b.pending, reg)
		b.live[reg] = reg
		reg.publishProgress(b.visibleTimeTick)
		return nil, b.visibleTimeTick, true, nil, nil
	}
	return batch, b.visibleTimeTick, false, nil, nil
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
		stream := b.owner.removeLocked(b.vchannel, b)
		b.mu.Unlock()
		b.owner.mu.Unlock()
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
			if reg.ctx.Err() == nil {
				b.fail(err)
				return
			}
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
	})
}

func (r *registration) finish(err error) {
	r.errMu.Lock()
	if err != nil {
		r.err = err
		close(r.progressNotify)
		r.progressNotify = make(chan struct{})
	}
	r.finishOnce.Do(func() { close(r.done) })
	r.errMu.Unlock()
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
	if err := r.segment.ApplyTransform(ctx, entry); err != nil {
		return err
	}
	r.publishProgress(entry.GetTimeTick())
	return nil
}

// WaitFailure observes failures after initial catch-up as well as during replay.
func (r *registration) WaitFailure(ctx context.Context) error {
	for {
		r.errMu.Lock()
		err, notify := r.err, r.progressNotify
		r.errMu.Unlock()
		if err != nil {
			return err
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
