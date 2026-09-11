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

package model

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SegmentChangeState is the lifecycle state of a SegmentChangeGroup.
//
// The group is the atomic publication unit of batch segment changes (import,
// mix/sort/clustering/forcemerge compaction, copy, external refresh, CDC): its
// members are staged in meta but invisible to every view until the whole group
// is atomically published into a DataView snapshot. See
// docs/design-docs/design_docs/20260909-datacoord-segment-change-staging-design.md.
type SegmentChangeState int32

const (
	// SegmentChangeStateStaged: members are in meta, async post-processing
	// (sort/stats/index) is running, invisible to views.
	SegmentChangeStateStaged SegmentChangeState = iota
	// SegmentChangeStateReady: all members passed the readiness predicate,
	// waiting for the atomic publish.
	SegmentChangeStateReady
	// SegmentChangeStateCommitted: members were atomically published into a
	// DataView snapshot (compact_version +1) and are visible.
	SegmentChangeStateCommitted
	// SegmentChangeStateFailed: a member's post-processing failed terminally;
	// members are reclaimed, superseded parents stay visible.
	SegmentChangeStateFailed
	// SegmentChangeStateAborted: timeout / external drop / collection drop;
	// members are reclaimed.
	SegmentChangeStateAborted
)

func (s SegmentChangeState) String() string {
	switch s {
	case SegmentChangeStateStaged:
		return "Staged"
	case SegmentChangeStateReady:
		return "Ready"
	case SegmentChangeStateCommitted:
		return "Committed"
	case SegmentChangeStateFailed:
		return "Failed"
	case SegmentChangeStateAborted:
		return "Aborted"
	default:
		return fmt.Sprintf("SegmentChangeState(%d)", int32(s))
	}
}

// SegmentChangeSource identifies which batch pipeline produced the group.
type SegmentChangeSource int32

const (
	SegmentChangeSourceImportJob SegmentChangeSource = iota + 1
	SegmentChangeSourceSortCompaction
	SegmentChangeSourceMixCompaction
	SegmentChangeSourceClustering
	SegmentChangeSourceForceMerge
	SegmentChangeSourceStorageVersion
	SegmentChangeSourceBumpSchema
	SegmentChangeSourceCopySegment
	SegmentChangeSourceExternalRefresh
	SegmentChangeSourceCDCReplicated
)

func (s SegmentChangeSource) String() string {
	switch s {
	case SegmentChangeSourceImportJob:
		return "ImportJob"
	case SegmentChangeSourceSortCompaction:
		return "SortCompaction"
	case SegmentChangeSourceMixCompaction:
		return "MixCompaction"
	case SegmentChangeSourceClustering:
		return "Clustering"
	case SegmentChangeSourceForceMerge:
		return "ForceMerge"
	case SegmentChangeSourceStorageVersion:
		return "StorageVersion"
	case SegmentChangeSourceBumpSchema:
		return "BumpSchema"
	case SegmentChangeSourceCopySegment:
		return "CopySegment"
	case SegmentChangeSourceExternalRefresh:
		return "ExternalRefresh"
	case SegmentChangeSourceCDCReplicated:
		return "CDCReplicated"
	default:
		return fmt.Sprintf("SegmentChangeSource(%d)", int32(s))
	}
}

// SegmentChangeGroup is one atomic publication unit of batch segment changes.
//
// This is a self-contained Go model (JSON-serialized under
// coord/segchange/{collectionID}/{groupID}); it mirrors the fields of the
// design's proposed datapb.SegmentChangeGroup and is intended to be replaced by
// that proto message once it lands, keeping the catalog/meta API stable.
type SegmentChangeGroup struct {
	GroupID      int64               `json:"groupID"`
	Source       SegmentChangeSource `json:"source"`
	CollectionID int64               `json:"collectionID"`
	PartitionID  int64               `json:"partitionID"` // 0 = cross-partition (import usually)
	SourceJobID  int64               `json:"sourceJobID"` // import jobID / compaction planID / copy taskID
	State        SegmentChangeState  `json:"state"`
	// NewSegmentIDs are the staged members published by this group.
	NewSegmentIDs []int64 `json:"newSegmentIds"`
	// SupersededSegmentIDs are the parents retired (marked Dropped) at publish.
	SupersededSegmentIDs []int64 `json:"supersededSegmentIds"`
	// SupersededL0SegmentIDs are the superseded parents that were exempted from
	// the anti-duplication invariant as L0 delta segments at REGISTRATION time.
	// The exemption decision is persisted so recovery is stable: an L0 parent
	// may be GC'd from SegmentMeta later, and the persisted list (not a live
	// segment-level lookup) must drive the recovery-time exemption — otherwise
	// identical persisted bytes would flip from conflict-free to a startup
	// brick once the L0 segment disappears (review C11).
	SupersededL0SegmentIDs []int64 `json:"supersededL0SegmentIds"`
	// CommitTS is allocated at publish; 0 while staged. Members share it so a
	// batch gets one temporal commit boundary.
	CommitTS uint64 `json:"commitTS"`
	// PublishEpoch is a monotonic counter of the publish txn; used for READY
	// replay detection after a crash ("txn committed but response lost").
	PublishEpoch int64  `json:"publishEpoch"`
	CreateTS     int64  `json:"createTS"`
	ReadyTS      int64  `json:"readyTS"`
	CommitTime   int64  `json:"commitTime"`
	FailReason   string `json:"failReason"`
}

// Clone returns a deep copy of the group.
func (g *SegmentChangeGroup) Clone() *SegmentChangeGroup {
	if g == nil {
		return nil
	}
	clone := *g
	clone.NewSegmentIDs = append([]int64(nil), g.NewSegmentIDs...)
	clone.SupersededSegmentIDs = append([]int64(nil), g.SupersededSegmentIDs...)
	clone.SupersededL0SegmentIDs = append([]int64(nil), g.SupersededL0SegmentIDs...)
	return &clone
}

// String returns a compact single-line description of the group for logs.
func (g *SegmentChangeGroup) String() string {
	if g == nil {
		return "<nil>"
	}
	return fmt.Sprintf("group(%d, coll=%d, src=%s, state=%s, new=%v, superseded=%v)",
		g.GroupID, g.CollectionID, g.Source, g.State, g.NewSegmentIDs, g.SupersededSegmentIDs)
}

// ContainsSegment reports whether id is a member (new or superseded) of g.
func (g *SegmentChangeGroup) ContainsSegment(id int64) bool {
	if g == nil {
		return false
	}
	for _, member := range g.NewSegmentIDs {
		if member == id {
			return true
		}
	}
	for _, member := range g.SupersededSegmentIDs {
		if member == id {
			return true
		}
	}
	return false
}

// IsTerminal reports whether the group reached a state from which it can no
// longer transition (published, failed, or aborted).
func (g *SegmentChangeGroup) IsTerminal() bool {
	if g == nil {
		return true
	}
	switch g.State {
	case SegmentChangeStateCommitted, SegmentChangeStateFailed, SegmentChangeStateAborted:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether next is a legal transition from the current
// state. A no-op (current == next) is legal so idempotent replays succeed.
func (g *SegmentChangeGroup) CanTransitionTo(next SegmentChangeState) bool {
	if g == nil {
		return false
	}
	switch g.State {
	case SegmentChangeStateStaged:
		return next == SegmentChangeStateStaged ||
			next == SegmentChangeStateReady ||
			next == SegmentChangeStateFailed ||
			next == SegmentChangeStateAborted
	case SegmentChangeStateReady:
		return next == SegmentChangeStateReady ||
			next == SegmentChangeStateCommitted ||
			next == SegmentChangeStateFailed ||
			next == SegmentChangeStateAborted
	case SegmentChangeStateCommitted, SegmentChangeStateFailed, SegmentChangeStateAborted:
		// Terminal states only accept themselves (idempotent replay).
		return next == g.State
	default:
		return false
	}
}

// Validate enforces the write-time invariants of the group. Violations are a
// caller programming error and must be rejected before any catalog write.
func (g *SegmentChangeGroup) Validate() error {
	if g == nil {
		return merr.WrapErrServiceInternalMsg("segment change group is nil")
	}
	// C21: bound the enum ranges. Every alive-group predicate in the meta layer
	// is a whitelist that skips anything outside STAGED/READY, so an out-of-range
	// State would never enter the reverse indexes — its members/superseded become
	// claimable by a second group (bypassing the anti-duplication invariant) —
	// while IsTerminal/CanTransitionTo both fall through to false, leaving the
	// record neither deletable nor able to transition.
	if g.State < SegmentChangeStateStaged || g.State > SegmentChangeStateAborted {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d has invalid state %d", g.GroupID, int32(g.State))
	}
	if g.Source < SegmentChangeSourceImportJob || g.Source > SegmentChangeSourceCDCReplicated {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d has invalid source %d", g.GroupID, int32(g.Source))
	}
	if g.GroupID <= 0 {
		return merr.WrapErrServiceInternalMsg("segment change group %d requires a positive group ID", g.GroupID)
	}
	if g.CollectionID <= 0 {
		return merr.WrapErrServiceInternalMsg("segment change group %d requires a positive collection ID", g.GroupID)
	}
	if len(g.NewSegmentIDs) == 0 {
		return merr.WrapErrServiceInternalMsg("segment change group %d has no new segments", g.GroupID)
	}
	for _, id := range g.NewSegmentIDs {
		if id <= 0 {
			return merr.WrapErrServiceInternalMsg("segment change group %d has invalid new segment %d", g.GroupID, id)
		}
	}
	// new_segment_ids and superseded_segment_ids must be disjoint.
	seen := make(map[int64]struct{}, len(g.NewSegmentIDs)+len(g.SupersededSegmentIDs))
	for _, id := range g.NewSegmentIDs {
		if _, dup := seen[id]; dup {
			return merr.WrapErrDataIntegrityMsg("segment change group %d duplicates new segment %d", g.GroupID, id)
		}
		seen[id] = struct{}{}
	}
	for _, id := range g.SupersededSegmentIDs {
		if id <= 0 {
			return merr.WrapErrServiceInternalMsg("segment change group %d has invalid superseded segment %d", g.GroupID, id)
		}
		if _, dup := seen[id]; dup {
			return merr.WrapErrDataIntegrityMsg("segment change group %d segment %d is both new and superseded", g.GroupID, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// MarshalSegmentChangeGroup serializes the group for etcd persistence.
func MarshalSegmentChangeGroup(g *SegmentChangeGroup) ([]byte, error) {
	if g == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot marshal a nil segment change group")
	}
	return json.Marshal(g)
}

// UnmarshalSegmentChangeGroup deserializes a group from etcd AND validates it.
// A malformed, zero-valued, or otherwise invalid record (e.g. an out-of-range
// State after a corrupt write or version downgrade) is returned as a
// data-integrity error so the kv catalog fails the recovery walk
// (fail-closed) instead of loading it: the alive-group whitelists in the meta
// layer silently skip an out-of-range State, orphaning its members and
// bypassing the anti-duplication invariant, while IsTerminal/CanTransitionTo
// both fall through false, leaving the record neither deletable nor
// transitionable (C34). A group is the sole owner of its members' visibility —
// SegmentInfo has no change_group_id field.
func UnmarshalSegmentChangeGroup(data []byte) (*SegmentChangeGroup, error) {
	group := &SegmentChangeGroup{}
	if err := json.Unmarshal(data, group); err != nil {
		return nil, err
	}
	if err := group.Validate(); err != nil {
		return nil, err
	}
	return group, nil
}
