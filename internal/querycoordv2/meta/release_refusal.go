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
	"sync"
	"time"
)

// ReleaseRefusal is one sealed segment whose release a delegator keeps refusing.
type ReleaseRefusal struct {
	CollectionID int64
	ReplicaID    int64
	SegmentID    int64
	// NodeID is the node the refused release named, i.e. the one that holds the segment.
	NodeID int64
	// Since is the first refusal; Last the most recent one.
	Since time.Time
	Last  time.Time
	// TargetVersion is the collection's current target version at the first refusal. A refusal
	// that outlives a target change is counted from the change, since a new current target is
	// progress for the replica.
	TargetVersion int64
}

type releaseRefusalKey struct {
	ReplicaID int64
	SegmentID int64
}

// ReleaseRefusalTracker remembers which releases the delegators refused, since when, and whether
// the replica has loaded anything since -- what the segment checker needs to decide a forced
// release. The task scheduler feeds it from the outcome of segment tasks: a release that fails
// with ErrSegmentReleaseRefused starts or extends a refusal, a release that goes through ends it,
// and a load that goes through is progress for its replica.
type ReleaseRefusalTracker struct {
	mu       sync.Mutex
	refusals map[releaseRefusalKey]*ReleaseRefusal
	// lastLoad is the last successful load per replica
	lastLoad map[int64]time.Time
}

func NewReleaseRefusalTracker() *ReleaseRefusalTracker {
	return &ReleaseRefusalTracker{
		refusals: make(map[releaseRefusalKey]*ReleaseRefusal),
		lastLoad: make(map[int64]time.Time),
	}
}

// Refused records that the delegator refused to release the segment at now, with the collection's
// current target version at that moment, and returns the refusal as it stands. The clock starts
// at the first refusal and restarts when the current target has moved since.
func (t *ReleaseRefusalTracker) Refused(collectionID, replicaID, segmentID, nodeID, targetVersion int64, now time.Time) ReleaseRefusal {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := releaseRefusalKey{ReplicaID: replicaID, SegmentID: segmentID}
	refusal, ok := t.refusals[key]
	if !ok || refusal.TargetVersion != targetVersion {
		refusal = &ReleaseRefusal{
			CollectionID:  collectionID,
			ReplicaID:     replicaID,
			SegmentID:     segmentID,
			Since:         now,
			TargetVersion: targetVersion,
		}
		t.refusals[key] = refusal
	}
	refusal.NodeID = nodeID
	refusal.Last = now
	return *refusal
}

// Released ends the refusal of the segment, if there was one, and reports how long it lasted.
func (t *ReleaseRefusalTracker) Released(replicaID, segmentID int64, now time.Time) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := releaseRefusalKey{ReplicaID: replicaID, SegmentID: segmentID}
	refusal, ok := t.refusals[key]
	if !ok {
		return 0, false
	}
	delete(t.refusals, key)
	return now.Sub(refusal.Since), true
}

// LoadSucceeded records progress for the replica: a segment was loaded at now.
func (t *ReleaseRefusalTracker) LoadSucceeded(replicaID int64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.After(t.lastLoad[replicaID]) {
		t.lastLoad[replicaID] = now
	}
}

// LastLoadSucceeded returns when the replica last loaded a segment, if it did since the tracker
// started.
func (t *ReleaseRefusalTracker) LastLoadSucceeded(replicaID int64) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	last, ok := t.lastLoad[replicaID]
	return last, ok
}

// Get returns the refusal of the segment in the replica, if any.
func (t *ReleaseRefusalTracker) Get(replicaID, segmentID int64) (ReleaseRefusal, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	refusal, ok := t.refusals[releaseRefusalKey{ReplicaID: replicaID, SegmentID: segmentID}]
	if !ok {
		return ReleaseRefusal{}, false
	}
	return *refusal, true
}

// ListByCollection returns the ongoing refusals of the collection.
func (t *ReleaseRefusalTracker) ListByCollection(collectionID int64) []ReleaseRefusal {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]ReleaseRefusal, 0)
	for _, refusal := range t.refusals {
		if refusal.CollectionID == collectionID {
			result = append(result, *refusal)
		}
	}
	return result
}

// Prune drops the refusals of the replica whose segment the predicate no longer knows -- the
// segment left the distribution some other way (a node went down, the collection was released).
func (t *ReleaseRefusalTracker) Prune(replicaID int64, inDist func(segmentID int64) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.refusals {
		if key.ReplicaID == replicaID && !inDist(key.SegmentID) {
			delete(t.refusals, key)
		}
	}
}

// RemoveReplica forgets everything about the replica.
func (t *ReleaseRefusalTracker) RemoveReplica(replicaID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.refusals {
		if key.ReplicaID == replicaID {
			delete(t.refusals, key)
		}
	}
	delete(t.lastLoad, replicaID)
}
