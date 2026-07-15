package transformlogbuffer

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type Buffer struct {
	streams wal.TransformLogStreamManager

	mu                sync.Mutex
	streamsByPChannel map[string]*streamState
	channels          map[string]*vchannelBuffer

	drainTasks chan *registration
}

func New(streams wal.TransformLogStreamManager) *Buffer {
	b := &Buffer{
		streams:           streams,
		streamsByPChannel: make(map[string]*streamState),
		channels:          make(map[string]*vchannelBuffer),
		drainTasks:        make(chan *registration, 1024),
	}
	for i := 0; i < 4; i++ {
		go b.drainWorker()
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
	defer b.mu.Unlock()
	buf := b.channels[vchannel]
	if buf == nil {
		stream, err := b.getOrCreateStreamLocked(ctx, pchannel)
		if err != nil {
			return nil, err
		}
		buf = newVChannelBuffer(b, pchannel, vchannel, startFrom)
		b.channels[vchannel] = buf
		stream.refs[vchannel] = buf
	}
	if err := buf.acquireLocked(startFrom); err != nil {
		return nil, err
	}
	stream := b.streamsByPChannel[pchannel]
	if stream != nil {
		buf.ensureSubscribed(stream.stream)
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
		return nil, fmt.Errorf("transform log buffer for vchannel %q is not acquired", segment.VChannel())
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
				_ = state.stream.Close()
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
	stream, err := b.streams.AcquireStream(ctx, pchannel)
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
		h.buffer.onEntry(event.Entry)
	}
	if event.CaughtUp != nil {
		mlog.Debug(context.TODO(), "querynode transform log buffer received caught-up",
			mlog.FieldPChannel(h.buffer.pchannel),
			mlog.FieldVChannel(h.buffer.vchannel),
			mlog.Int64("subscriptionID", event.SubscriptionID),
			mlog.Uint64("startAfterTimeTick", event.CaughtUp.StartAfterTimeTick),
		)
		h.buffer.onCaughtUp()
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
	sub      wal.TransformLogSubscription

	initCtx    context.Context
	initCancel context.CancelFunc
	initOnce   sync.Once
	closing    bool

	mu               sync.Mutex
	retentionStart   uint64
	visibleTimeTick  uint64
	visibilityNotify chan struct{}
	guards           map[uint64]int
	entries          []*streamingpb.TransformLogEntry
	live             map[int64]*registration
	pending          map[int64]*registration
	caughtUp         bool
	err              error
}

func newVChannelBuffer(owner *Buffer, pchannel string, vchannel string, startFrom uint64) *vchannelBuffer {
	initCtx, initCancel := context.WithCancel(context.Background())
	return &vchannelBuffer{
		owner:            owner,
		pchannel:         pchannel,
		vchannel:         vchannel,
		initCtx:          initCtx,
		initCancel:       initCancel,
		retentionStart:   startFrom,
		visibleTimeTick:  startFrom,
		visibilityNotify: make(chan struct{}),
		guards:           make(map[uint64]int),
		live:             make(map[int64]*registration),
		pending:          make(map[int64]*registration),
	}
}

func (b *vchannelBuffer) ensureSubscribed(stream wal.TransformLogStream) {
	b.initOnce.Do(func() {
		go b.subscribe(stream)
	})
}

func (b *vchannelBuffer) subscribe(stream wal.TransformLogStream) {
	sub, err := stream.Subscribe(b.initCtx, wal.TransformLogSubscriptionOption{
		VChannel:           b.vchannel,
		StartAfterTimeTick: b.retentionStart,
		Handler:            bufEventHandler{buffer: b},
	})
	if err != nil {
		if b.isClosing() && errors.Is(err, context.Canceled) {
			return
		}
		b.fail(err)
		return
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		_ = sub.Close()
		return
	}
	b.sub = sub
	b.mu.Unlock()
	mlog.Debug(context.TODO(), "querynode transform log buffer subscribed vchannel",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.Uint64("startAfterTimeTick", b.retentionStart),
		mlog.Int64("subscriptionID", sub.ID()),
	)
}

func (b *vchannelBuffer) isClosing() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closing
}

func (b *vchannelBuffer) acquireLocked(startFrom uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return context.Canceled
	}
	if b.err != nil {
		return b.err
	}
	if startFrom < b.retentionStart {
		return fmt.Errorf("transform log buffer range starts from %d, cannot serve %d", b.retentionStart, startFrom)
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
		return nil, fmt.Errorf("transform log buffer range starts from %d, cannot serve segment %d from %d", b.retentionStart, segment.ID(), startFrom)
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

	if err := b.owner.scheduleDrain(ctx, reg); err != nil {
		b.removeRegistration(reg)
		reg.finish(err)
		return nil, err
	}
	return reg, nil
}

func (b *vchannelBuffer) drainRegistration(ctx context.Context, reg *registration) error {
	for {
		batch, done, notify, err := b.nextCatchupBatch(reg)
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
			if err := reg.segment.ApplyTransform(ctx, entry); err != nil {
				return err
			}
			if entry.GetTimeTick() > reg.drainedTo {
				reg.drainedTo = entry.GetTimeTick()
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
	if b.pending[reg.segment.ID()] != reg {
		return nil, true, nil, nil
	}
	batch := make([]*streamingpb.TransformLogEntry, 0)
	for _, entry := range b.entries {
		if entry.GetTimeTick() > reg.drainedTo {
			batch = append(batch, entry)
		}
	}
	if len(batch) == 0 {
		if !b.caughtUp {
			return nil, false, b.visibilityNotify, nil
		}
		delete(b.pending, reg.segment.ID())
		b.live[reg.segment.ID()] = reg
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
				mlog.Bool("caughtUp", b.caughtUp),
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
				mlog.Bool("caughtUp", b.caughtUp),
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
	delete(b.pending, reg.segment.ID())
	if b.live[reg.segment.ID()] == reg {
		delete(b.live, reg.segment.ID())
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
		b.closing = true
		if b.initCancel != nil {
			b.initCancel()
		}
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
		if err := reg.segment.ApplyTransform(context.Background(), entry); err != nil {
			b.fail(err)
			return
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

func (b *vchannelBuffer) onCaughtUp() {
	b.mu.Lock()
	if b.caughtUp {
		b.mu.Unlock()
		return
	}
	b.caughtUp = true
	mlog.Debug(context.TODO(), "querynode transform log buffer marked caught-up",
		mlog.FieldPChannel(b.pchannel),
		mlog.FieldVChannel(b.vchannel),
		mlog.Uint64("visibleTimeTick", b.visibleTimeTick),
	)
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
	buffer     *vchannelBuffer
	segment    qnview.TransformSegment
	startFrom  uint64
	drainedTo  uint64
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	err        error
	errMu      sync.Mutex
	once       sync.Once
	finishOnce sync.Once
}

func newRegistration(buffer *vchannelBuffer, segment qnview.TransformSegment) *registration {
	ctx, cancel := context.WithCancel(context.Background())
	return &registration{
		buffer:    buffer,
		segment:   segment,
		startFrom: segment.TransformStartAfterTimeTick(),
		drainedTo: segment.TransformStartAfterTimeTick(),
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
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
				mlog.Uint64("drainedTo", r.drainedTo),
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
			mlog.Uint64("drainedTo", r.drainedTo),
			mlog.Err(ctx.Err()),
		)
		return ctx.Err()
	}
}

func (r *registration) Unregister() {
	r.once.Do(func() {
		r.cancel()
		r.buffer.unregister(r)
	})
}

func (r *registration) finish(err error) {
	r.finishOnce.Do(func() {
		r.errMu.Lock()
		r.err = err
		r.errMu.Unlock()
		close(r.done)
	})
}
