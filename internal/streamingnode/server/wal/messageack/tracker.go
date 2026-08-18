package messageack

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

const maxStallCheckInterval = time.Second

// VChannelDataPersister asynchronously schedules persistence for buffered data
// of one VChannel through the requested TimeTick.
type VChannelDataPersister interface {
	RequestPersistThrough(vchannel string, targetTimeTick uint64)
}

type trackedEntry struct {
	point     utility.WALConsumeCheckpoint
	vchannel  string
	message   message.ImmutableMessage
	trackedAt time.Time
	completed bool
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
	mu             sync.Mutex
	completedPoint utility.WALConsumeCheckpoint
	pending        []*trackedEntry
	vchannels      map[string]*vchannelPending
	onAdvance      func(utility.WALConsumeCheckpoint)
	dataPersister  VChannelDataPersister
}

func NewTracker(
	initial utility.WALConsumeCheckpoint,
	onAdvance func(utility.WALConsumeCheckpoint),
	dataPersister VChannelDataPersister,
) *Tracker {
	return &Tracker{
		completedPoint: initial,
		vchannels:      make(map[string]*vchannelPending),
		onAdvance:      onAdvance,
		dataPersister:  dataPersister,
	}
}

func (t *Tracker) Track(raw message.ImmutableMessage) message.OwnedImmutableMessage {
	entry := &trackedEntry{
		point: utility.WALConsumeCheckpoint{
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
func (t *Tracker) Run(ctx context.Context, stallTimeout time.Duration) {
	if stallTimeout <= 0 || t.dataPersister == nil {
		<-ctx.Done()
		return
	}
	interval := stallTimeout
	if interval > maxStallCheckInterval {
		interval = maxStallCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.triggerStalledVChannels(now, stallTimeout)
		}
	}
}

func (t *Tracker) triggerStalledVChannels(now time.Time, stallTimeout time.Duration) {
	requests := t.collectStalledPersistRequests(now, stallTimeout)
	for _, request := range requests {
		t.dataPersister.RequestPersistThrough(request.vchannel, request.targetTimeTick)
	}
}

func (t *Tracker) collectStalledPersistRequests(now time.Time, stallTimeout time.Duration) []persistRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	requests := make([]persistRequest, 0)
	for vchannel, state := range t.vchannels {
		t.compactVChannelPendingLocked(vchannel, state)
		if len(state.pending) == 0 {
			continue
		}
		var maxStalledTimeTick uint64
		hasStalledMessage := false
		for _, entry := range state.pending {
			if entry.completed || now.Sub(entry.trackedAt) < stallTimeout {
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

func (t *Tracker) CompletedPoint() utility.WALConsumeCheckpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return *t.completedPoint.Clone()
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

func (t *Tracker) completeLocked(entry *trackedEntry) (func(utility.WALConsumeCheckpoint), utility.WALConsumeCheckpoint, bool) {
	entry.completed = true

	completed := 0
	for completed < len(t.pending) && t.pending[completed].completed {
		completed++
	}
	if completed == 0 {
		return nil, utility.WALConsumeCheckpoint{}, false
	}
	point := *t.pending[completed-1].point.Clone()
	clear(t.pending[:completed])
	t.pending = t.pending[completed:]
	if !shouldAdvance(t.completedPoint, point) {
		return nil, utility.WALConsumeCheckpoint{}, false
	}
	t.completedPoint = point
	return t.onAdvance, point, true
}

func shouldAdvance(current, next utility.WALConsumeCheckpoint) bool {
	if next.TimeTick != current.TimeTick {
		return next.TimeTick > current.TimeTick
	}
	if current.MessageID == nil {
		return next.MessageID != nil
	}
	return next.MessageID != nil && current.MessageID.LT(next.MessageID)
}
