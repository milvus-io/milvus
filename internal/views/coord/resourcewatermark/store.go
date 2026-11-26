package resourcewatermark

import (
	"math"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const MaxAge = 3 * qviews.ViewFeedbackInterval

// Sample is node-wide and independent of collection/view lifetimes. ReceivedAt
// uses the coordinator's clock; no worker clock synchronization is required.
type Sample struct {
	Memory, Disk float64
	ReceivedAt   time.Time
}

type Reader interface {
	Read(node int64, now time.Time) (Sample, bool)
}

// Store deliberately has no observer or dirty notification: resource changes
// are inputs to existing reconciliation, never a reason to schedule balance.
type Store struct {
	mu    sync.RWMutex
	nodes map[int64]entry
	next  uint64
}

type entry struct {
	Sample
	generation uint64
}

func New() *Store { return &Store{nodes: make(map[int64]entry)} }

// Start fences previous streams, including their eventual disconnect callbacks.
func (s *Store) Start(node int64) func(*viewpb.NodeResourceWatermarks) {
	s.mu.Lock()
	s.next++
	generation := s.next
	s.nodes[node] = entry{generation: generation}
	s.mu.Unlock()
	return func(report *viewpb.NodeResourceWatermarks) { s.update(node, generation, report) }
}

// update accepts reports only from the current stream. nil invalidates on disconnect.
func (s *Store) update(node int64, generation uint64, report *viewpb.NodeResourceWatermarks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.nodes[node]; !ok || current.generation != generation {
		return
	}
	if report == nil {
		delete(s.nodes, node)
		return
	}
	if !valid(report.Memory) || !valid(report.Disk) {
		s.nodes[node] = entry{generation: generation}
		return
	}
	s.nodes[node] = entry{Sample: Sample{Memory: report.Memory, Disk: report.Disk, ReceivedAt: time.Now()}, generation: generation}
}

func valid(value float64) bool { return !math.IsNaN(value) && value >= 0 && value <= 100 }

func (s *Store) Read(node int64, now time.Time) (Sample, bool) {
	s.mu.RLock()
	sample, ok := s.nodes[node]
	s.mu.RUnlock()
	return sample.Sample, ok && !sample.ReceivedAt.IsZero() && now.Sub(sample.ReceivedAt) <= MaxAge
}
