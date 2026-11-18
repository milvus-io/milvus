package balancer

import (
	"maps"
	"sync"

	"google.golang.org/grpc/attributes"
)

type availabilityKey struct{}

// NodeAvailability publishes the per-server readiness of one gRPC client's picker.
type NodeAvailability struct {
	mu          sync.Mutex
	ready       map[int64]bool
	unavailable []int64
	changed     chan struct{}
}

func NewNodeAvailability() *NodeAvailability {
	return &NodeAvailability{changed: make(chan struct{})}
}

func (n *NodeAvailability) ResolverAttributes() *attributes.Attributes {
	return attributes.New(availabilityKey{}, n)
}

// Snapshot returns an immutable list and a notification for the next membership
// or readiness change. An unknown server is handled on its first failed pick.
func (n *NodeAvailability) Snapshot() ([]int64, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.unavailable, n.changed
}

func (n *NodeAvailability) publish(ready map[int64]bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if maps.Equal(n.ready, ready) {
		return
	}
	var unavailable []int64
	for id, ok := range ready {
		if !ok {
			unavailable = append(unavailable, id)
		}
	}
	n.ready, n.unavailable = ready, unavailable
	close(n.changed)
	n.changed = make(chan struct{})
}
