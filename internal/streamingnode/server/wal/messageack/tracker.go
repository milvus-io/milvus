package messageack

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const maxStallCheckInterval = time.Second

// VChannelPersistRequester asynchronously schedules persistence for buffered data
// of one VChannel through the requested TimeTick.
type VChannelPersistRequester interface {
	RequestPersistThrough(vchannel string, targetTimeTick uint64)
}

type trackedEntry struct {
	point            utility.WALCheckpoint
	logicalEndOffset uint64
	vchannel         string
	message          message.ImmutableMessage
	trackedAt        time.Time
	completed        bool
}

type vchannelPending struct {
	pending                 []*trackedEntry
	persistRequested        bool
	persistRequestedThrough uint64
}

type persistRequest struct {
	vchannel       string
	targetTimeTick uint64
}

type Tracker struct {
	mu                     sync.Mutex
	completedPoint         utility.WALCheckpoint
	observedLogicalOffset  uint64
	completedLogicalOffset uint64
	pending                []*trackedEntry
	vchannels              map[string]*vchannelPending
	onAdvance              func(utility.WALCheckpoint)
	persistRequester       VChannelPersistRequester
}

func NewTracker(
	initial utility.WALCheckpoint,
	onAdvance func(utility.WALCheckpoint),
	persistRequester VChannelPersistRequester,
) *Tracker {
	return &Tracker{
		completedPoint:   initial,
		vchannels:        make(map[string]*vchannelPending),
		onAdvance:        onAdvance,
		persistRequester: persistRequester,
	}
}

func (t *Tracker) Track(raw message.ImmutableMessage) message.OwnedImmutableMessage {
	entry := &trackedEntry{
		point: utility.WALCheckpoint{
			MessageID: raw.LastConfirmedMessageID(),
			TimeTick:  raw.TimeTick(),
		},
		vchannel:  raw.VChannel(),
		message:   raw,
		trackedAt: time.Now(),
	}
	owner := message.NewOwnedImmutableMessage(raw, func() {
		t.complete(entry)
	})

	t.mu.Lock()
	if shouldAdvance(t.completedPoint, entry.point) {
		t.observedLogicalOffset = saturatingAdd(t.observedLogicalOffset, logicalMessageSize(raw))
	}
	entry.logicalEndOffset = t.observedLogicalOffset
	t.pending = append(t.pending, entry)
	if entry.vchannel != "" {
		state := t.vchannels[entry.vchannel]
		if state == nil {
			state = &vchannelPending{}
			t.vchannels[entry.vchannel] = state
		}
		state.pending = append(state.pending, entry)
	}
	t.mu.Unlock()
	return owner
}

// Run detects VChannel-scoped acknowledgement stalls until ctx is canceled.
func (t *Tracker) Run(ctx context.Context, stallTimeout time.Duration, underPressure func() bool) {
	if (stallTimeout <= 0 && underPressure == nil) || t.persistRequester == nil {
		<-ctx.Done()
		return
	}
	interval := maxStallCheckInterval
	if stallTimeout > 0 && stallTimeout < interval {
		interval = stallTimeout
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			force := underPressure != nil && underPressure()
			t.triggerVChannels(now, stallTimeout, force)
		}
	}
}

func (t *Tracker) triggerStalledVChannels(now time.Time, stallTimeout time.Duration) {
	t.triggerVChannels(now, stallTimeout, false)
}

func (t *Tracker) triggerVChannels(now time.Time, stallTimeout time.Duration, force bool) {
	requests := t.collectPersistRequests(now, stallTimeout, force)
	for _, request := range requests {
		t.persistRequester.RequestPersistThrough(request.vchannel, request.targetTimeTick)
	}
}

func (t *Tracker) collectStalledPersistRequests(now time.Time, stallTimeout time.Duration) []persistRequest {
	return t.collectPersistRequests(now, stallTimeout, false)
}

func (t *Tracker) collectPersistRequests(now time.Time, stallTimeout time.Duration, force bool) []persistRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	var pressureHead *trackedEntry
	if force && len(t.pending) > 0 {
		// Byte pressure asks only the VChannel that owns the oldest incomplete
		// global-prefix blocker. Component batching may persist newer data; the
		// Tracker must not widen the target to unrelated VChannels or messages.
		pressureHead = t.pending[0]
	}
	requests := make([]persistRequest, 0)
	for vchannel, state := range t.vchannels {
		t.compactVChannelPendingLocked(vchannel, state)
		if len(state.pending) == 0 {
			continue
		}
		var maxStalledTimeTick uint64
		hasStalledMessage := false
		for _, entry := range state.pending {
			stalled := stallTimeout > 0 && now.Sub(entry.trackedAt) >= stallTimeout
			if entry.completed || (!stalled && entry != pressureHead) {
				continue
			}
			hasStalledMessage = true
			if entry.point.TimeTick > maxStalledTimeTick {
				maxStalledTimeTick = entry.point.TimeTick
			}
		}
		if !hasStalledMessage ||
			(state.persistRequested && state.persistRequestedThrough >= maxStalledTimeTick) {
			continue
		}
		state.persistRequested = true
		state.persistRequestedThrough = maxStalledTimeTick
		requests = append(requests, persistRequest{
			vchannel:       vchannel,
			targetTimeTick: maxStalledTimeTick,
		})
	}
	return requests
}

func (t *Tracker) CompletedPoint() utility.WALCheckpoint {
	point, _ := t.Completed()
	return point
}

// Completed returns the continuous completed WAL point and its runtime logical
// end offset. The offset is relative to the checkpoint from which this Tracker
// started and is intentionally not part of the durable checkpoint format.
func (t *Tracker) Completed() (utility.WALCheckpoint, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return *t.completedPoint.Clone(), t.completedLogicalOffset
}

// LogicalOffsets returns the observed and continuous completed runtime byte
// frontiers. Both offsets are relative to the Tracker's initial checkpoint.
func (t *Tracker) LogicalOffsets() (observed, completed uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.observedLogicalOffset, t.completedLogicalOffset
}

func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func (t *Tracker) complete(entry *trackedEntry) {
	t.mu.Lock()
	if entry.completed {
		t.mu.Unlock()
		return
	}
	entry.message = nil
	onAdvance, point, advanced := t.completeLocked(entry)
	if entry.vchannel != "" {
		if state := t.vchannels[entry.vchannel]; state != nil {
			t.compactVChannelPendingLocked(entry.vchannel, state)
		}
	}
	t.mu.Unlock()
	if advanced && onAdvance != nil {
		onAdvance(point)
	}
}

func (t *Tracker) compactVChannelPendingLocked(vchannel string, state *vchannelPending) {
	completed := 0
	for completed < len(state.pending) && state.pending[completed].completed {
		completed++
	}
	if completed > 0 {
		clear(state.pending[:completed])
		state.pending = state.pending[completed:]
	}
	if len(state.pending) == 0 {
		delete(t.vchannels, vchannel)
	}
}

func (t *Tracker) completeLocked(entry *trackedEntry) (func(utility.WALCheckpoint), utility.WALCheckpoint, bool) {
	entry.completed = true

	completed := 0
	for completed < len(t.pending) && t.pending[completed].completed {
		completed++
	}
	if completed == 0 {
		return nil, utility.WALCheckpoint{}, false
	}
	point := *t.pending[completed-1].point.Clone()
	completedLogicalOffset := t.pending[completed-1].logicalEndOffset
	clear(t.pending[:completed])
	t.pending = t.pending[completed:]
	t.completedLogicalOffset = completedLogicalOffset
	if !shouldAdvance(t.completedPoint, point) {
		return nil, utility.WALCheckpoint{}, false
	}
	t.completedPoint = point
	return t.onAdvance, point, true
}

func logicalMessageSize(msg message.ImmutableMessage) uint64 {
	size := msg.EstimateSize()
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func shouldAdvance(current, next utility.WALCheckpoint) bool {
	if next.TimeTick != current.TimeTick {
		return next.TimeTick > current.TimeTick
	}
	if current.MessageID == nil {
		return next.MessageID != nil
	}
	return next.MessageID != nil && current.MessageID.LT(next.MessageID)
}
