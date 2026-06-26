package transformlogbuffer

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type Buffer struct {
	accesser wal.TransformLogAccesser

	mu       sync.Mutex
	channels map[string]*vchannelBuffer
}

func New(accesser wal.TransformLogAccesser) *Buffer {
	return &Buffer{
		accesser: accesser,
		channels: make(map[string]*vchannelBuffer),
	}
}

func (b *Buffer) Acquire(ctx context.Context, view *qviews.QueryViewAtQueryNode) (qnview.TransformLogGuard, error) {
	if view == nil {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	meta := view.IntoProto().GetMeta()
	vchannel := meta.GetVchannel()
	startFrom := meta.GetDeleteApplyStartAfterTimetick()
	if vchannel == "" {
		return nil, wal.ErrTransformLogInvalidReadOption
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	buf := b.channels[vchannel]
	if buf == nil {
		scannerCtx, cancel := context.WithCancel(context.Background())
		buf = newVChannelBuffer(b, vchannel, startFrom, cancel)
		b.channels[vchannel] = buf
		buf.start(scannerCtx, b.accesser)
	}
	if err := buf.acquire(ctx, startFrom); err != nil {
		return nil, err
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

func (b *Buffer) remove(vchannel string, buf *vchannelBuffer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.channels[vchannel] == buf {
		delete(b.channels, vchannel)
	}
}

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

type vchannelBuffer struct {
	owner    *Buffer
	vchannel string
	cancel   context.CancelFunc

	mu             sync.Mutex
	scanner        wal.TransformLogScanner
	retentionStart uint64
	guards         map[uint64]int
	entries        []*streamingpb.TransformLogEntry
	regs           map[*registration]struct{}
	caughtUp       bool
	err            error
}

func newVChannelBuffer(owner *Buffer, vchannel string, startFrom uint64, cancel context.CancelFunc) *vchannelBuffer {
	return &vchannelBuffer{
		owner:          owner,
		vchannel:       vchannel,
		cancel:         cancel,
		retentionStart: startFrom,
		guards:         make(map[uint64]int),
		regs:           make(map[*registration]struct{}),
	}
}

func (b *vchannelBuffer) start(ctx context.Context, accesser wal.TransformLogAccesser) {
	scanner := accesser.Read(ctx, wal.TransformLogReadOption{
		Name:               fmt.Sprintf("qv-transformlog-%s", b.vchannel),
		VChannel:           b.vchannel,
		StartAfterTimeTick: b.retentionStart,
	})
	b.mu.Lock()
	b.scanner = scanner
	b.mu.Unlock()
	go b.consume(ctx, scanner)
}

func (b *vchannelBuffer) acquire(_ context.Context, startFrom uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refreshScannerDoneLocked()
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
	defer b.mu.Unlock()
	b.refreshScannerDoneLocked()
	if b.err != nil {
		return nil, b.err
	}
	startFrom := segment.TransformStartAfterTimeTick()
	if startFrom < b.retentionStart {
		return nil, fmt.Errorf("transform log buffer range starts from %d, cannot serve segment %d from %d", b.retentionStart, segment.ID(), startFrom)
	}
	reg := newRegistration(ctx, b, segment)
	for _, entry := range b.entries {
		if entry.GetTimeTick() > startFrom {
			reg.enqueue(regEvent{entry: entry})
		}
	}
	if b.caughtUp {
		reg.enqueue(regEvent{caughtUp: true})
	}
	b.regs[reg] = struct{}{}
	return reg, nil
}

func (b *vchannelBuffer) unregister(reg *registration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.regs, reg)
}

func (b *vchannelBuffer) releaseGuard(startFrom uint64) {
	b.mu.Lock()
	if count := b.guards[startFrom]; count > 1 {
		b.guards[startFrom] = count - 1
		b.trimLocked()
		b.mu.Unlock()
		return
	}
	delete(b.guards, startFrom)
	if len(b.guards) == 0 {
		scanner := b.scanner
		b.mu.Unlock()
		b.cancel()
		if scanner != nil {
			_ = scanner.Close()
		}
		b.owner.remove(b.vchannel, b)
		return
	}
	b.trimLocked()
	b.mu.Unlock()
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

func (b *vchannelBuffer) consume(ctx context.Context, scanner wal.TransformLogScanner) {
	for {
		select {
		case event, ok := <-scanner.Chan():
			if !ok {
				b.fail(scannerErr(scanner))
				return
			}
			if event.Entry != nil {
				b.onEntry(event.Entry)
			}
			if event.CaughtUp != nil {
				b.onCaughtUp()
			}
		case <-scanner.Done():
			b.fail(scannerErr(scanner))
			return
		case <-ctx.Done():
			b.fail(ctx.Err())
			return
		}
	}
}

func (b *vchannelBuffer) onEntry(entry *streamingpb.TransformLogEntry) {
	b.mu.Lock()
	if entry.GetTimeTick() > b.retentionStart {
		b.entries = append(b.entries, entry)
	}
	regs := make([]*registration, 0, len(b.regs))
	for reg := range b.regs {
		if entry.GetTimeTick() > reg.startFrom {
			regs = append(regs, reg)
		}
	}
	b.mu.Unlock()

	for _, reg := range regs {
		reg.enqueue(regEvent{entry: entry})
	}
}

func (b *vchannelBuffer) onCaughtUp() {
	b.mu.Lock()
	if b.caughtUp {
		b.mu.Unlock()
		return
	}
	b.caughtUp = true
	regs := make([]*registration, 0, len(b.regs))
	for reg := range b.regs {
		regs = append(regs, reg)
	}
	b.mu.Unlock()

	for _, reg := range regs {
		reg.enqueue(regEvent{caughtUp: true})
	}
}

func (b *vchannelBuffer) fail(err error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return
	}
	b.err = err
	regs := make([]*registration, 0, len(b.regs))
	for reg := range b.regs {
		regs = append(regs, reg)
	}
	b.mu.Unlock()

	for _, reg := range regs {
		reg.enqueue(regEvent{err: err})
	}
}

func (b *vchannelBuffer) refreshScannerDoneLocked() {
	if b.err != nil || b.scanner == nil {
		return
	}
	select {
	case <-b.scanner.Done():
		b.err = scannerErr(b.scanner)
	default:
	}
}

type regEvent struct {
	entry    *streamingpb.TransformLogEntry
	caughtUp bool
	err      error
}

type registration struct {
	buffer    *vchannelBuffer
	segment   qnview.TransformSegment
	startFrom uint64

	events chan regEvent
	done   chan error
	stop   chan struct{}
	once   sync.Once
}

func newRegistration(ctx context.Context, buffer *vchannelBuffer, segment qnview.TransformSegment) *registration {
	reg := &registration{
		buffer:    buffer,
		segment:   segment,
		startFrom: segment.TransformStartAfterTimeTick(),
		events:    make(chan regEvent, 1024),
		done:      make(chan error, 1),
		stop:      make(chan struct{}),
	}
	go reg.consume(ctx)
	return reg
}

func (r *registration) WaitCatchup(ctx context.Context) error {
	select {
	case err := <-r.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *registration) Unregister() {
	r.once.Do(func() {
		close(r.stop)
		r.buffer.unregister(r)
	})
}

func (r *registration) enqueue(event regEvent) {
	select {
	case r.events <- event:
	case <-r.stop:
	}
}

func (r *registration) consume(ctx context.Context) {
	for {
		select {
		case event := <-r.events:
			if event.err != nil {
				r.finish(event.err)
				return
			}
			if event.entry != nil {
				if err := r.segment.ApplyTransform(ctx, event.entry); err != nil {
					r.finish(err)
					return
				}
			}
			if event.caughtUp {
				r.finish(nil)
			}
		case <-r.stop:
			return
		case <-ctx.Done():
			r.finish(ctx.Err())
			return
		}
	}
}

func (r *registration) finish(err error) {
	select {
	case r.done <- err:
	default:
	}
}

func scannerErr(scanner wal.TransformLogScanner) error {
	if err := scanner.Error(); err != nil {
		return err
	}
	return io.EOF
}
