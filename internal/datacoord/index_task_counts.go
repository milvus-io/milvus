// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

var indexTaskMetricStates = [...]indexpb.JobState{
	indexpb.JobState_JobStateNone,
	indexpb.JobState_JobStateInit,
	indexpb.JobState_JobStateInProgress,
	indexpb.JobState_JobStateFinished,
	indexpb.JobState_JobStateFailed,
	indexpb.JobState_JobStateRetry,
}

type indexTaskStateCounts [len(indexTaskMetricStates)]int64

// indexTaskStats aggregates the task records of one (collection, index).
type indexTaskStats struct {
	counts indexTaskStateCounts
	// size sums IndexSerializedSize over every task record, deleted tasks and
	// all states included, which is what DataCoordStoredIndexFilesSize reports.
	size int64
}

// indexTaskCounts uses a short, memory-only lock independent of fieldIndexLock.
// Manifest installs can update it under segMu without waiting for index DDL
// persistence. It adds no per-build objects or pointers.
// Raw counts and sizes include orphaned/dropped field indexes; counts exclude
// deleted tasks. Keeping them allows index DDL to adjust totals without visiting
// builds.
type indexTaskCounts struct {
	mu      sync.RWMutex
	active  map[[2]int64]bool
	byIndex map[[2]int64]indexTaskStats
	total   indexTaskStateCounts
	// activeSizes is the per-collection size of active indexes, the value of
	// DataCoordStoredIndexFilesSize. Zero sums are not stored.
	activeSizes map[int64]int64
	// recovering holds back gauge writes while reload rebuilds the aggregate.
	recovering bool
}

func indexTaskStateSlot(state commonpb.IndexState) int {
	switch state {
	case commonpb.IndexState_IndexStateNone:
		return 0
	case commonpb.IndexState_Unissued:
		return 1
	case commonpb.IndexState_InProgress:
		return 2
	case commonpb.IndexState_Finished:
		return 3
	case commonpb.IndexState_Failed:
		return 4
	case commonpb.IndexState_Retry:
		return 5
	default:
		return -1
	}
}

// adjustTask reports whether the active size of task's collection changed.
func (c *indexTaskCounts) adjustTask(task *model.SegmentIndex, delta int64) bool {
	if task == nil {
		return false
	}
	key := [2]int64{task.CollectionID, task.IndexID}
	active := c.active[key]
	stats := c.byIndex[key]
	if slot := indexTaskStateSlot(task.IndexState); slot >= 0 && !task.IsDeleted {
		stats.counts[slot] += delta
		if active {
			c.total[slot] += delta
		}
	}
	size := delta * int64(task.IndexSerializedSize)
	stats.size += size
	if stats == (indexTaskStats{}) {
		delete(c.byIndex, key)
	} else {
		if c.byIndex == nil {
			c.byIndex = make(map[[2]int64]indexTaskStats)
		}
		c.byIndex[key] = stats
	}
	if !active || size == 0 {
		return false
	}
	c.addActiveSize(task.CollectionID, size)
	return true
}

func (c *indexTaskCounts) addActiveSize(collectionID, delta int64) {
	if c.activeSizes == nil {
		c.activeSizes = make(map[int64]int64)
	}
	if size := c.activeSizes[collectionID] + delta; size == 0 {
		delete(c.activeSizes, collectionID)
	} else {
		c.activeSizes[collectionID] = size
	}
}

// publishStoredSize sets the absolute value, so a series deleted by collection
// cleanup is rebuilt correctly by the next update. The caller holds mu.
func (c *indexTaskCounts) publishStoredSize(collectionID int64) {
	if c.recovering {
		return
	}
	metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", strconv.FormatInt(collectionID, 10)).
		Set(float64(c.activeSizes[collectionID]))
}

func (c *indexTaskCounts) replaceTask(old, current *model.SegmentIndex) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old != nil && current != nil && old.CollectionID == current.CollectionID &&
		old.IndexID == current.IndexID && old.IndexState == current.IndexState &&
		old.IsDeleted == current.IsDeleted && old.IndexSerializedSize == current.IndexSerializedSize {
		return
	}
	oldChanged := c.adjustTask(old, -1)
	currentChanged := c.adjustTask(current, 1)
	if oldChanged {
		c.publishStoredSize(old.CollectionID)
	}
	if currentChanged && (!oldChanged || current.CollectionID != old.CollectionID) {
		c.publishStoredSize(current.CollectionID)
	}
}

func (c *indexTaskCounts) replaceIndex(old, current *model.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		c.active = make(map[[2]int64]bool)
	}
	// Publish after both changes so replacing a live definition never exposes
	// an intermediate total.
	var changed [2]int64
	n := 0
	for _, change := range [...]struct {
		index *model.Index
		delta int64
	}{{old, -1}, {current, 1}} {
		if change.index == nil || change.index.IsDeleted {
			continue
		}
		key := [2]int64{change.index.CollectionID, change.index.IndexID}
		if change.delta < 0 {
			delete(c.active, key)
		} else {
			c.active[key] = true
		}
		stats := c.byIndex[key]
		for state, count := range stats.counts {
			c.total[state] += count * change.delta
		}
		if stats.size != 0 {
			c.addActiveSize(key[0], stats.size*change.delta)
			changed[n] = key[0]
			n++
		}
	}
	for i := 0; i < n; i++ {
		if i == 0 || changed[i] != changed[0] {
			c.publishStoredSize(changed[i])
		}
	}
}

// setRecovering holds back or resumes gauge writes around reload.
func (c *indexTaskCounts) setRecovering(recovering bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recovering = recovering
}

// publishRecoveredStoredSize publishes a collection rebuilt by reload. A live
// update may have published it already; both write the current total.
func (c *indexTaskCounts) publishRecoveredStoredSize(collectionID int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.activeSizes[collectionID]; ok {
		c.publishStoredSize(collectionID)
	}
}

// forgetStoredSize deletes the gauge series of a collection whose last index
// definition is gone; with no active index left it holds no size. The caller
// holds fieldIndexLock so no index of the collection can be activated
// concurrently.
func (c *indexTaskCounts) forgetStoredSize(collectionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	metrics.DataCoordStoredIndexFilesSize.DeleteLabelValues("", "", strconv.FormatInt(collectionID, 10))
}

func (c *indexTaskCounts) snapshot() indexTaskStateCounts {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.total
}
