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
	// dirtySizes holds the collections whose stored size has not been
	// published since it last changed; publication fills in their sizes.
	dirtySizes map[int64]int64
	// publishMu orders gauge writes; see publishStoredSizes.
	publishMu sync.Mutex
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

func (c *indexTaskCounts) adjustTask(task *model.SegmentIndex, delta int64) {
	if task == nil {
		return
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
	if active && size != 0 {
		c.addActiveSize(task.CollectionID, size)
	}
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
	if c.dirtySizes == nil {
		c.dirtySizes = make(map[int64]int64)
	}
	c.dirtySizes[collectionID] = 0
}

func (c *indexTaskCounts) replaceTask(old, current *model.SegmentIndex) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old != nil && current != nil && old.CollectionID == current.CollectionID &&
		old.IndexID == current.IndexID && old.IndexState == current.IndexState &&
		old.IsDeleted == current.IsDeleted && old.IndexSerializedSize == current.IndexSerializedSize {
		return
	}
	c.adjustTask(old, -1)
	c.adjustTask(current, 1)
}

func (c *indexTaskCounts) replaceIndex(old, current *model.Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		c.active = make(map[[2]int64]bool)
	}
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
		}
	}
}

// publishStoredSizes writes the current stored size of every dirty collection,
// deleting the series of one that has none, so a series deleted by collection
// cleanup is rebuilt correctly by the next change. Callers run it after
// releasing segMu and fieldIndexLock: creating or deleting a series waits for an
// in-flight scrape of the gauge vector. publishMu orders publications and each
// reads the totals while holding it, so an older total never overwrites a newer
// one.
func (c *indexTaskCounts) publishStoredSizes() {
	c.mu.RLock()
	idle := len(c.dirtySizes) == 0
	c.mu.RUnlock()
	if idle {
		// The publication that took this caller's change reads the total
		// after it.
		return
	}
	c.publishMu.Lock()
	defer c.publishMu.Unlock()
	c.mu.Lock()
	sizes := c.dirtySizes
	c.dirtySizes = nil
	for collectionID := range sizes {
		sizes[collectionID] = c.activeSizes[collectionID]
	}
	c.mu.Unlock()
	for collectionID, size := range sizes {
		label := strconv.FormatInt(collectionID, 10)
		if size == 0 {
			metrics.DataCoordStoredIndexFilesSize.DeleteLabelValues("", "", label)
		} else {
			metrics.DataCoordStoredIndexFilesSize.WithLabelValues("", "", label).Set(float64(size))
		}
	}
}

func (c *indexTaskCounts) snapshot() indexTaskStateCounts {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.total
}
