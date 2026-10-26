package replicadiscovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Snapshot is a replacement routing hint. Valid distinguishes an empty candidate
// set from disconnected/uninitialized state. Replicas are owned by the receiver.
type Snapshot struct {
	Replicas []Replica
	Valid    bool
}

// Replica describes a shard in the subscribed VChannel. Its maximum Up version
// is a routing-change hint, not the version pinned by a particular query.
// Value fields keep snapshots independent without retaining mutable protobufs.
type Replica struct {
	ReplicaID        int64
	MaxUpViewVersion qviews.QueryViewVersion
}

func replicasFromProto(entries []*viewpb.VChannelReplicas_Replica) []Replica {
	replicas := make([]Replica, len(entries))
	for i, entry := range entries {
		replicas[i] = Replica{ReplicaID: entry.GetReplicaId(), MaxUpViewVersion: qviews.FromProtoQueryViewVersion(entry.GetMaxUpViewVersion())}
	}
	return replicas
}

// Handler runs serially on a shared PChannel dispatcher, outside manager locks.
// It must only update local routing state, never wait for I/O or call Manager.Close.
// Subscription.Close is allowed. A callback already selected may finish after
// Subscription.Close; Manager.Close joins all callbacks.
type Handler func(Snapshot)

// Manager shares one logical watcher per PChannel across all its subscriptions.
// Idle watchers retain only their parked control loops, not a physical stream.
type Manager struct {
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	watchers  map[string]*watcher
	factory   func(context.Context, string) (Stream, error)
}

func NewManager(factory func(context.Context, string) (Stream, error)) *Manager {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Manager.Close owns cancellation.
	return &Manager{ctx: ctx, cancel: cancel, watchers: make(map[string]*watcher), factory: factory}
}

func (m *Manager) Subscribe(vchannel string, handler Handler) (*Subscription, error) {
	if vchannel == "" || handler == nil {
		return nil, status.NewInvalidRequestSeq("replica subscription requires VChannel and handler")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ctx.Err(); err != nil {
		return nil, err
	}
	pc := funcutil.ToPhysicalChannel(vchannel)
	w := m.watchers[pc]
	if w == nil {
		w = newWatcher(m.ctx, pc, func(ctx context.Context) (Stream, error) { return m.factory(ctx, pc) })
		m.watchers[pc] = w
	}
	return w.subscribe(vchannel, handler)
}

func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.cancel()
		watchers := make([]*watcher, 0, len(m.watchers))
		for _, w := range m.watchers {
			watchers = append(watchers, w)
		}
		m.mu.Unlock()
		for _, w := range watchers {
			w.Close()
		}
		m.mu.Lock()
		m.watchers = nil
		m.mu.Unlock()
	})
}
