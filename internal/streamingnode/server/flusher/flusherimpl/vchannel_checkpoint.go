package flusherimpl

import (
	"container/heap"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var (
	errVChannelAlreadyExists = errors.New("vchannel already exists")
	errVChannelNotFound      = errors.New("vchannel not found")
	errRollbackCheckpoint    = errors.New("rollback a checkpoint is not allow")
)

// newVChannelCheckpointManager creates a new vchannelCheckpointManager
func newVChannelCheckpointManager(exists map[string]message.MessageID) *vchannelCheckpointManager {
	index := make(map[string]*vchannelCheckpoint)
	checkpointHeap := make(vchannelCheckpointHeap, 0, len(exists))
	for vchannel, checkpoint := range exists {
		index[vchannel] = &vchannelCheckpoint{
			vchannel:   vchannel,
			checkpoint: checkpoint,
			index:      len(checkpointHeap),
		}
		checkpointHeap = append(checkpointHeap, index[vchannel])
	}
	heap.Init(&checkpointHeap)
	return &vchannelCheckpointManager{
		checkpointHeap: checkpointHeap,
		index:          index,
	}
}

// vchannelCheckpointManager is the struct to manage the checkpoints of all vchannels at one pchannel
type vchannelCheckpointManager struct {
	checkpointHeap vchannelCheckpointHeap
	index          map[string]*vchannelCheckpoint
}

// Add adds a vchannel with a checkpoint to the manager
func (m *vchannelCheckpointManager) Add(vchannel string, checkpoint message.MessageID) error {
	if _, ok := m.index[vchannel]; ok {
		return errVChannelAlreadyExists
	}
	vc := &vchannelCheckpoint{
		vchannel:   vchannel,
		checkpoint: checkpoint,
	}
	heap.Push(&m.checkpointHeap, vc)
	m.index[vchannel] = vc
	return nil
}

// Drop removes a vchannel from the manager
func (m *vchannelCheckpointManager) Drop(vchannel string) error {
	vc, ok := m.index[vchannel]
	if !ok {
		return errVChannelNotFound
	}
	heap.Remove(&m.checkpointHeap, vc.index)
	delete(m.index, vchannel)
	return nil
}

// Update updates the checkpoint of a vchannel
func (m *vchannelCheckpointManager) Update(vchannel string, checkpoint message.MessageID) error {
	previous, ok := m.index[vchannel]
	if !ok {
		return errVChannelNotFound
	}
	if checkpoint.LT(previous.checkpoint) {
		return errors.Wrapf(errRollbackCheckpoint, "checkpoint: %s, previous: %s", checkpoint, previous.checkpoint)
	}
	if checkpoint.EQ(previous.checkpoint) {
		return nil
	}
	m.checkpointHeap.Update(previous, checkpoint)
	return nil
}

// Len returns the number of vchannels
func (m *vchannelCheckpointManager) Len() int {
	return len(m.checkpointHeap)
}

// MinimumCheckpoint returns the minimum checkpoint of all vchannels
func (m *vchannelCheckpointManager) MinimumCheckpoint() message.MessageID {
	if len(m.checkpointHeap) == 0 {
		return nil
	}
	return m.checkpointHeap[0].checkpoint
}

// vchannelCheckpoint is the struct to hold the checkpoint of a vchannel
type vchannelCheckpoint struct {
	vchannel   string
	checkpoint message.MessageID
	index      int
}

// A vchannelCheckpointHeap implements heap.Interface and holds Items.
type vchannelCheckpointHeap []*vchannelCheckpoint

func (pq vchannelCheckpointHeap) Len() int { return len(pq) }

func (pq vchannelCheckpointHeap) Less(i, j int) bool {
	return pq[i].checkpoint.LT(pq[j].checkpoint)
}

func (pq vchannelCheckpointHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *vchannelCheckpointHeap) Push(x any) {
	n := len(*pq)
	item := x.(*vchannelCheckpoint)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *vchannelCheckpointHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *vchannelCheckpointHeap) Update(item *vchannelCheckpoint, checkpoint message.MessageID) {
	item.checkpoint = checkpoint
	heap.Fix(pq, item.index)
}
