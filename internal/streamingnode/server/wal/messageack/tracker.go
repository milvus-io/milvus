package messageack

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type trackedEntry struct {
	point     utility.WALConsumeCheckpoint
	message   message.ImmutableMessage
	completed bool
}

type Tracker struct {
	mu             sync.Mutex
	completedPoint utility.WALConsumeCheckpoint
	pending        []*trackedEntry
	onAdvance      func(utility.WALConsumeCheckpoint)
}

func NewTracker(initial utility.WALConsumeCheckpoint, onAdvance func(utility.WALConsumeCheckpoint)) *Tracker {
	return &Tracker{
		completedPoint: initial,
		onAdvance:      onAdvance,
	}
}

func (t *Tracker) Track(raw message.ImmutableMessage) message.OwnedImmutableMessage {
	entry := &trackedEntry{
		point: utility.WALConsumeCheckpoint{
			MessageID: raw.LastConfirmedMessageID(),
			TimeTick:  raw.TimeTick(),
		},
		message: raw,
	}
	owner := message.NewOwnedImmutableMessage(raw, func() {
		t.complete(entry)
	})

	t.mu.Lock()
	t.pending = append(t.pending, entry)
	t.mu.Unlock()
	return owner
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
	t.mu.Unlock()
	if advanced && onAdvance != nil {
		onAdvance(point)
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
