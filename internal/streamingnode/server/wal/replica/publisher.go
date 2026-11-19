package replica

import (
	"context"
	"io"
	"maps"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Publisher lives for one writer term. It keeps one immutable signal per
// active View. A slow reader retains at most its preceding snapshot, including
// keys it may need to retire, without retaining the intervening event history.
type Publisher struct {
	topology      *streamingpb.WALReplicaTopology
	members       map[int64]bool
	confirmed     bool
	reportChanged chan struct{}

	mu            sync.Mutex
	checkpoint    *streamingpb.WALCheckpoint
	views         map[qviews.QueryViewKey]*streamingpb.WALQueryViewSignal
	subscriptions map[int64]*Subscription
	changed       chan struct{}
	closed        bool
}

func NewPublisher(checkpoint *streamingpb.WALCheckpoint) *Publisher {
	p := &Publisher{members: make(map[int64]bool), reportChanged: make(chan struct{}), views: make(map[qviews.QueryViewKey]*streamingpb.WALQueryViewSignal), subscriptions: make(map[int64]*Subscription), changed: make(chan struct{})}
	p.PublishCheckpoint(checkpoint)
	return p
}

func (p *Publisher) notifyLocked() { close(p.changed); p.changed = make(chan struct{}) }

func (p *Publisher) PublishCheckpoint(cp *streamingpb.WALCheckpoint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || cp == nil || (p.checkpoint != nil && cp.TimeTick <= p.checkpoint.TimeTick) {
		return
	}
	p.checkpoint = &streamingpb.WALCheckpoint{TimeTick: cp.TimeTick, MessageId: cp.MessageId}
	p.notifyLocked()
}

func (p *Publisher) PublishView(key qviews.QueryViewKey, state viewpb.QueryViewState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if state == viewpb.QueryViewState_QueryViewStateDropped {
		delete(p.views, key)
		for _, sub := range p.subscriptions {
			delete(sub.reports, key)
		}
	} else {
		// A repeated Down may strengthen preempt_serving_lease. Publish a new
		// signal even when the enum has not changed so readers reload control.
		p.views[key] = &streamingpb.WALQueryViewSignal{View: &viewpb.QueryViewKey{Shard: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()}, State: state}
	}
	p.notifyLocked()
}

// Subscribe replaces any older stream from this replica. The first response and
// registration have one linearization point under the writer's lock.
func (p *Publisher) Subscribe(id int64) (*Subscription, *streamingpb.WALReplicaState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.members[id] {
		return nil, nil, io.EOF
	}
	if old := p.subscriptions[id]; old != nil {
		close(old.done)
	}
	s := &Subscription{publisher: p, id: id, done: make(chan struct{}), reports: make(map[qviews.QueryViewKey]viewpb.QueryViewState)}
	p.subscriptions[id] = s
	p.notifyReportsLocked()
	snapshot := s.diffLocked()
	return s, snapshot, nil
}

type Subscription struct {
	publisher  *Publisher
	id         int64
	done       chan struct{}
	previous   map[qviews.QueryViewKey]*streamingpb.WALQueryViewSignal
	checkpoint *streamingpb.WALCheckpoint
	reports    map[qviews.QueryViewKey]viewpb.QueryViewState
}

func (s *Subscription) diffLocked() *streamingpb.WALReplicaState {
	p := s.publisher
	change := &streamingpb.WALReplicaState{}
	if p.checkpoint != s.checkpoint {
		change.Checkpoint = p.checkpoint
		s.checkpoint = p.checkpoint
	}
	for key, signal := range p.views {
		if s.previous[key] != signal {
			change.QueryViews = append(change.QueryViews, signal)
		}
	}
	for key, signal := range s.previous {
		if p.views[key] == nil {
			change.QueryViews = append(change.QueryViews, &streamingpb.WALQueryViewSignal{View: signal.View, State: viewpb.QueryViewState_QueryViewStateDropped})
		}
	}
	s.previous = maps.Clone(p.views)
	return change
}

func (s *Subscription) Next(ctx context.Context) (*streamingpb.WALReplicaState, error) {
	p := s.publisher
	for {
		p.mu.Lock()
		if p.subscriptions[s.id] != s {
			p.mu.Unlock()
			return nil, io.EOF
		}
		changes := s.diffLocked()
		changed := p.changed
		p.mu.Unlock()
		if changes.Checkpoint != nil || len(changes.QueryViews) > 0 {
			return changes, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.done:
			return nil, io.EOF
		case <-changed:
		}
	}
}

func (s *Subscription) Report(report *streamingpb.WALQueryViewReport) {
	p := s.publisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subscriptions[s.id] != s {
		return
	}
	key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(report.View.Shard), QueryViewVersion: qviews.FromProtoQueryViewVersion(report.View.Version)}
	if p.views[key] != nil && s.reports[key] != report.LocalState {
		s.reports[key] = report.LocalState
		p.notifyReportsLocked()
	}
}

func (s *Subscription) Close() {
	p := s.publisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subscriptions[s.id] == s {
		delete(p.subscriptions, s.id)
		close(s.done)
		p.notifyReportsLocked()
	}
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, s := range p.subscriptions {
		close(s.done)
	}
	clear(p.subscriptions)
	p.notifyReportsLocked()
	clear(p.views)
	p.checkpoint = nil
}

// SetMembers applies the current control connection's membership confirmation.
// Replacing the control connection clears confirmation until Coord rechecks it.
func (p *Publisher) SetMembers(topology *streamingpb.WALReplicaTopology, confirmed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	clear(p.members)
	for _, member := range topology.GetMembers() {
		if member.WalReplicaId != topology.WriterReplicaId {
			p.members[member.WalReplicaId] = true
		}
	}
	for id, s := range p.subscriptions {
		if !p.members[id] {
			delete(p.subscriptions, id)
			close(s.done)
		}
	}
	p.topology = topology
	p.confirmed = confirmed
	p.notifyReportsLocked()
}

func (p *Publisher) notifyReportsLocked() {
	close(p.reportChanged)
	p.reportChanged = make(chan struct{})
}

// WaitReports waits for acknowledgements on the current streams. Losing a
// stream withdraws its earlier report until a replacement reports again.
func (p *Publisher) WaitReports(ctx context.Context, key qviews.QueryViewKey, want viewpb.QueryViewState) error {
	return p.WithReports(ctx, key, want, nil)
}

// WithReports serializes the conditional catalog transaction with changes in
// membership and reports on the currently connected streams.
func (p *Publisher) WithReports(ctx context.Context, key qviews.QueryViewKey, want viewpb.QueryViewState, commit func(*streamingpb.WALReplicaTopology) error) error {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return io.EOF
		}
		ready := p.confirmed
		for id := range p.members {
			s := p.subscriptions[id]
			if s == nil {
				ready = false
				break
			}
			report := s.reports[key]
			matches := report == want || (want == viewpb.QueryViewState_QueryViewStateDown && report == viewpb.QueryViewState_QueryViewStateDropped) || (want == viewpb.QueryViewState_QueryViewStateReady && report == viewpb.QueryViewState_QueryViewStateUp)
			if !matches {
				ready = false
				break
			}
		}
		changed := p.reportChanged
		if ready {
			err := ctx.Err()
			if err == nil && commit != nil {
				err = commit(p.topology)
			}
			p.mu.Unlock()
			return err
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
