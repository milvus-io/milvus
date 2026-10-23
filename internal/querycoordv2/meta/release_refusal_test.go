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

package meta

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A refusal is counted from the first refusal under a target version: later refusals keep the
// start, a target change restarts it, a release ends it and reports how long it lasted.
func TestReleaseRefusalTrackerCountsFromFirstRefusal(t *testing.T) {
	tracker := NewReleaseRefusalTracker()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	first := tracker.Refused(1, 10, 100, 5, 7, start)
	assert.Equal(t, start, first.Since)
	assert.Equal(t, int64(5), first.NodeID)

	later := tracker.Refused(1, 10, 100, 6, 7, start.Add(time.Minute))
	assert.Equal(t, start, later.Since, "a later refusal keeps the first one's start")
	assert.Equal(t, start.Add(time.Minute), later.Last)
	assert.Equal(t, int64(6), later.NodeID, "the node is the one the latest refusal named")

	restarted := tracker.Refused(1, 10, 100, 6, 8, start.Add(2*time.Minute))
	assert.Equal(t, start.Add(2*time.Minute), restarted.Since, "a new current target restarts the clock")
	assert.Equal(t, int64(8), restarted.TargetVersion)

	got, ok := tracker.Get(10, 100)
	assert.True(t, ok)
	assert.Equal(t, restarted, got)
	_, ok = tracker.Get(10, 101)
	assert.False(t, ok)

	refusedFor, ok := tracker.Released(10, 100, start.Add(5*time.Minute))
	assert.True(t, ok)
	assert.Equal(t, 3*time.Minute, refusedFor)
	_, ok = tracker.Get(10, 100)
	assert.False(t, ok)
	_, ok = tracker.Released(10, 100, start.Add(5*time.Minute))
	assert.False(t, ok, "a release of a segment that was not refused reports nothing")
}

// Progress is per replica and never goes backwards.
func TestReleaseRefusalTrackerProgress(t *testing.T) {
	tracker := NewReleaseRefusalTracker()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	_, ok := tracker.LastLoadSucceeded(10)
	assert.False(t, ok)
	tracker.LoadSucceeded(10, start.Add(time.Minute))
	tracker.LoadSucceeded(10, start)
	last, ok := tracker.LastLoadSucceeded(10)
	assert.True(t, ok)
	assert.Equal(t, start.Add(time.Minute), last)
	_, ok = tracker.LastLoadSucceeded(11)
	assert.False(t, ok)
}

// Listing is by collection; pruning drops the refusals of segments that left the distribution;
// removing a replica forgets its refusals and its progress.
func TestReleaseRefusalTrackerListPruneRemove(t *testing.T) {
	tracker := NewReleaseRefusalTracker()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tracker.Refused(1, 10, 100, 5, 7, now)
	tracker.Refused(1, 10, 101, 5, 7, now)
	tracker.Refused(2, 20, 200, 5, 7, now)
	tracker.LoadSucceeded(10, now)

	assert.Len(t, tracker.ListByCollection(1), 2)
	assert.Len(t, tracker.ListByCollection(2), 1)
	assert.Empty(t, tracker.ListByCollection(3))

	tracker.Prune(10, func(segmentID int64) bool { return segmentID == 101 })
	_, ok := tracker.Get(10, 100)
	assert.False(t, ok)
	_, ok = tracker.Get(10, 101)
	assert.True(t, ok)
	_, ok = tracker.Get(20, 200)
	assert.True(t, ok, "pruning one replica leaves the others alone")

	tracker.RemoveReplica(10)
	assert.Empty(t, tracker.ListByCollection(1))
	_, ok = tracker.LastLoadSucceeded(10)
	assert.False(t, ok)
	assert.Len(t, tracker.ListByCollection(2), 1)
}
