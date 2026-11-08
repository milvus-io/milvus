package snview

import (
	"cmp"
	"slices"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// replicaDirectory belongs to one WAL owner. It advertises only locally serving
// replicas and tracks preparing replicas to defer subscription initialization.
// All listener registration and publication is serialized by mu. No consumer
// callback, network operation, or shard lock is taken under this lock.
type replicaDirectory struct {
	mu        sync.Mutex
	replicas  map[string]map[int64]qviews.QueryViewVersion
	preparing map[string]map[int64]bool
	listeners map[string]map[*replicaWatch]struct{}
	done      chan struct{}
	closed    bool
}

func newReplicaDirectory() *replicaDirectory {
	return &replicaDirectory{replicas: make(map[string]map[int64]qviews.QueryViewVersion), preparing: make(map[string]map[int64]bool), listeners: make(map[string]map[*replicaWatch]struct{}), done: make(chan struct{})}
}

// Set publishes the maximum locally Up version and whether the shard has any
// Preparing, Ready or UpRecovering views. Nil removes its serving eligibility.
// The version is copied, never retained by pointer.
func (d *replicaDirectory) Set(vchannel string, replicaID int64, version *qviews.QueryViewVersion, preparing bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	replicas := d.replicas[vchannel]
	old, exists := replicas[replicaID]
	unchanged := (version == nil && !exists) || (version != nil && exists && old.EQ(*version))
	oldPreparing := len(d.preparing[vchannel]) > 0
	if preparing {
		if d.preparing[vchannel] == nil {
			d.preparing[vchannel] = make(map[int64]bool)
		}
		d.preparing[vchannel][replicaID] = true
	} else {
		delete(d.preparing[vchannel], replicaID)
		if len(d.preparing[vchannel]) == 0 {
			delete(d.preparing, vchannel)
		}
	}
	if unchanged && (len(replicas) > 0 || oldPreparing == (len(d.preparing[vchannel]) > 0)) {
		return
	}
	if version != nil {
		if replicas == nil {
			replicas = make(map[int64]qviews.QueryViewVersion)
			d.replicas[vchannel] = replicas
		}
		replicas[replicaID] = *version
	} else {
		delete(replicas, replicaID)
		if len(replicas) == 0 {
			delete(d.replicas, vchannel)
		}
	}
	for w := range d.listeners[vchannel] {
		w.enqueueLocked(vchannel)
	}
}

// Watch allocates a multiplexed listener, not a goroutine or a resource lease.
func (d *replicaDirectory) Watch() *replicaWatch {
	return &replicaWatch{directory: d, subscribed: make(map[string]struct{}), pending: make(map[string]struct{}), changed: make(chan struct{}, 1)}
}

func (d *replicaDirectory) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		d.closed = true
		d.replicas = nil
		d.preparing = nil
		d.listeners = nil
		close(d.done)
	}
}

// Watch is owned by one physical RPC. Pending changes coalesce by VChannel.
type replicaWatch struct {
	directory  *replicaDirectory
	subscribed map[string]struct{}
	pending    map[string]struct{}
	changed    chan struct{}
	closed     bool
}

func (w *replicaWatch) Done() <-chan struct{}    { return w.directory.done }
func (w *replicaWatch) Changed() <-chan struct{} { return w.changed }

func (w *replicaWatch) Subscribe(vchannel string) {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || w.closed {
		return
	}
	if _, ok := w.subscribed[vchannel]; ok {
		return
	}
	w.subscribed[vchannel] = struct{}{}
	if d.listeners[vchannel] == nil {
		d.listeners[vchannel] = make(map[*replicaWatch]struct{})
	}
	d.listeners[vchannel][w] = struct{}{}
	w.enqueueLocked(vchannel)
}

func (w *replicaWatch) Unsubscribe(vchannel string) {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	w.unsubscribeLocked(vchannel)
}

func (w *replicaWatch) unsubscribeLocked(vchannel string) {
	delete(w.pending, vchannel)
	delete(w.subscribed, vchannel)
	delete(w.directory.listeners[vchannel], w)
	if len(w.directory.listeners[vchannel]) == 0 {
		delete(w.directory.listeners, vchannel)
	}
}

func (w *replicaWatch) enqueueLocked(vchannel string) {
	w.pending[vchannel] = struct{}{}
	select {
	case w.changed <- struct{}{}:
	default:
	}
}

type replicaDirectorySnapshot struct {
	*viewpb.VChannelReplicas
	preparing bool
}

// Pop returns an owned latest snapshot. Read and publication share the same lock.
func (w *replicaWatch) Pop() *replicaDirectorySnapshot {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || w.closed {
		return nil
	}
	for vc := range w.pending {
		delete(w.pending, vc)
		result := &viewpb.VChannelReplicas{Vchannel: vc, Replicas: make([]*viewpb.VChannelReplicas_Replica, 0, len(d.replicas[vc]))}
		for id, version := range d.replicas[vc] {
			result.Replicas = append(result.Replicas, &viewpb.VChannelReplicas_Replica{ReplicaId: id, MaxUpViewVersion: version.IntoProto()})
		}
		slices.SortFunc(result.Replicas, func(a, b *viewpb.VChannelReplicas_Replica) int { return cmp.Compare(a.ReplicaId, b.ReplicaId) })
		// One message per wake leaves room for incoming unsubscribe requests.
		if len(w.pending) != 0 {
			select {
			case w.changed <- struct{}{}:
			default:
			}
		}
		return &replicaDirectorySnapshot{VChannelReplicas: result, preparing: len(d.preparing[vc]) > 0}
	}
	return nil
}

func (w *replicaWatch) Close() {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	w.closed = true
	for vc := range w.subscribed {
		w.unsubscribeLocked(vc)
	}
}
