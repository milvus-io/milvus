// Package replicadiscovery implements the latest-state VChannel replica directory.
package replicadiscovery

import (
	"cmp"
	"slices"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Directory belongs to one WAL owner. It contains only locally serving replicas.
// All listener registration and publication is serialized by mu. No consumer
// callback, network operation, or shard lock is taken under this lock.
type Directory struct {
	mu        sync.Mutex
	replicas  map[string]map[int64]qviews.QueryViewVersion
	listeners map[string]map[*Watch]struct{}
	done      chan struct{}
	closed    bool
}

func NewDirectory() *Directory {
	return &Directory{replicas: make(map[string]map[int64]qviews.QueryViewVersion), listeners: make(map[string]map[*Watch]struct{}), done: make(chan struct{})}
}

// Set publishes the maximum locally Up version; nil removes the replica.
// The version is copied, never retained by pointer.
func (d *Directory) Set(vchannel string, replicaID int64, version *qviews.QueryViewVersion) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	replicas := d.replicas[vchannel]
	old, exists := replicas[replicaID]
	if (version == nil && !exists) || (version != nil && exists && old.EQ(*version)) {
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
func (d *Directory) Watch() *Watch {
	return &Watch{directory: d, subscribed: make(map[string]struct{}), pending: make(map[string]struct{}), changed: make(chan struct{}, 1)}
}

func (d *Directory) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		d.closed = true
		d.replicas = nil
		d.listeners = nil
		close(d.done)
	}
}

// Watch is owned by one physical RPC. Pending changes coalesce by VChannel.
type Watch struct {
	directory  *Directory
	subscribed map[string]struct{}
	pending    map[string]struct{}
	changed    chan struct{}
	closed     bool
}

func (w *Watch) Done() <-chan struct{}    { return w.directory.done }
func (w *Watch) Changed() <-chan struct{} { return w.changed }

func (w *Watch) Subscribe(vchannel string) {
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
		d.listeners[vchannel] = make(map[*Watch]struct{})
	}
	d.listeners[vchannel][w] = struct{}{}
	w.enqueueLocked(vchannel)
}

func (w *Watch) Unsubscribe(vchannel string) {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	w.unsubscribeLocked(vchannel)
}

func (w *Watch) unsubscribeLocked(vchannel string) {
	delete(w.pending, vchannel)
	delete(w.subscribed, vchannel)
	delete(w.directory.listeners[vchannel], w)
	if len(w.directory.listeners[vchannel]) == 0 {
		delete(w.directory.listeners, vchannel)
	}
}

func (w *Watch) enqueueLocked(vchannel string) {
	w.pending[vchannel] = struct{}{}
	select {
	case w.changed <- struct{}{}:
	default:
	}
}

// Pop returns an owned latest snapshot. Read and publication share the same lock.
func (w *Watch) Pop() *viewpb.VChannelReplicas {
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
		return result
	}
	return nil
}

func (w *Watch) Close() {
	d := w.directory
	d.mu.Lock()
	defer d.mu.Unlock()
	w.closed = true
	for vc := range w.subscribed {
		w.unsubscribeLocked(vc)
	}
}
