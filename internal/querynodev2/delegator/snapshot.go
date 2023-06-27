// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delegator

import (
	"sync"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

// SnapshotItem group segmentEntry slice
type SnapshotItem struct {
	NodeID   int64
	Segments []SegmentEntry
}

// snapshotCleanup cleanup function signature.
type snapshotCleanup func()

// snapshot records segment distribution with ref count.
type snapshot struct {
	dist          []SnapshotItem
	growing       []SegmentEntry
	targetVersion int64

	// version ID for tracking
	version int64

	// signal channel for this snapshot cleared
	cleared chan struct{}
	once    sync.Once

	// reference to last version
	last *snapshot

	// reference count for this snapshot
	inUse atomic.Int64

	// expired flag
	expired atomic.Bool
}

// NewSnapshot returns a prepared snapshot with channel initialized.
func NewSnapshot(sealed []SnapshotItem, growing []SegmentEntry, last *snapshot, version int64, targetVersion int64) *snapshot {
	return &snapshot{
		version:       version,
		growing:       growing,
		dist:          sealed,
		last:          last,
		cleared:       make(chan struct{}),
		targetVersion: targetVersion,
	}
}

// Expire sets expired flag to true.
func (s *snapshot) Expire(cleanup snapshotCleanup) {
	s.expired.Store(true)
	s.checkCleared(cleanup)
}

// Get returns segment distributions with provided partition ids.
func (s *snapshot) Get(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry) {
	s.inUse.Inc()

	return s.filter(partitions...)
}

func (s *snapshot) GetTargetVersion() int64 {
	return s.targetVersion
}

// Peek returns segment distributions without increasing inUse.
func (s *snapshot) Peek(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry) {
	return s.filter(partitions...)
}

func (s *snapshot) filter(partitions ...int64) (sealed []SnapshotItem, growing []SegmentEntry) {
	filter := func(entry SegmentEntry, idx int) bool {
		return len(partitions) == 0 || funcutil.SliceContain(partitions, entry.PartitionID)
	}

	sealed = make([]SnapshotItem, 0, len(s.dist))
	for _, item := range s.dist {
		segments := lo.Filter(item.Segments, filter)
		sealed = append(sealed, SnapshotItem{
			NodeID:   item.NodeID,
			Segments: segments,
		})
	}

	growing = lo.Filter(s.growing, filter)
	return
}

// Done decreases inUse count for snapshot.
// also performs cleared check.
func (s *snapshot) Done(cleanup snapshotCleanup) {
	s.inUse.Dec()
	s.checkCleared(cleanup)
}

// checkCleared performs safety check for snapshot closing the cleared signal.
func (s *snapshot) checkCleared(cleanup snapshotCleanup) {
	if s.expired.Load() && s.inUse.Load() == 0 {
		s.once.Do(func() {
			// first snapshot
			if s.last == nil {
				close(s.cleared)
				cleanup()
				return
			}

			// wait last version cleared
			go func() {
				<-s.last.cleared
				s.last = nil
				cleanup()
				close(s.cleared)
			}()
		})
	}
}
