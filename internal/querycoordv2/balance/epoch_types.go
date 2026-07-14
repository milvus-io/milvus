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

package balance

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type SegmentObjectKey struct {
	ReplicaID int64
	SegmentID int64
	Scope     querypb.DataScope
}

func (k SegmentObjectKey) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%d/%d/%d", k.ReplicaID, k.SegmentID, k.Scope)), nil
}

type ChannelObjectKey struct {
	ReplicaID int64
	Channel   string
}

func (k ChannelObjectKey) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%d/%s", k.ReplicaID, k.Channel)), nil
}

type SnapshotToken struct {
	ResourceGroup         string
	RGHash                uint64
	ReplicaHash           uint64
	NodeHash              uint64
	LeaderHash            uint64
	PlacementHash         uint64
	SegmentRevision       int64
	ChannelRevision       int64
	PendingTaskRevision   uint64
	PendingGlobalRevision uint64
	CurrentTargetVersion  map[int64]int64
	NextTargetVersion     map[int64]int64

	pendingEpochRevisions map[task.BalanceEpochMeta]uint64
}

func (t SnapshotToken) Equal(other SnapshotToken) bool {
	return t.ResourceGroup == other.ResourceGroup &&
		t.RGHash == other.RGHash &&
		t.ReplicaHash == other.ReplicaHash &&
		t.NodeHash == other.NodeHash &&
		t.LeaderHash == other.LeaderHash &&
		t.PlacementHash == other.PlacementHash &&
		t.PendingTaskRevision == other.PendingTaskRevision &&
		equalInt64Map(t.CurrentTargetVersion, other.CurrentTargetVersion) &&
		equalInt64Map(t.NextTargetVersion, other.NextTargetVersion)
}

func (t SnapshotToken) PendingRevision(epoch task.BalanceEpochMeta) task.BalancePendingRevision {
	epochRevision := uint64(0)
	if epoch.ResourceGroup == t.ResourceGroup && epoch.ResourceGroup != "" &&
		(epoch.LeaderTerm != 0 || epoch.Sequence != 0) {
		epochRevision = t.pendingEpochRevisions[epoch]
	}
	return task.BalancePendingRevision{
		ResourceGroup: t.ResourceGroup,
		Epoch:         epoch,
		Revision:      t.PendingTaskRevision,
		EpochRevision: epochRevision,
	}
}

func (t SnapshotToken) WithPendingRevision(revision task.BalancePendingRevision) SnapshotToken {
	clone := cloneSnapshotToken(t)
	clone.PendingTaskRevision = revision.Revision
	if clone.pendingEpochRevisions == nil {
		clone.pendingEpochRevisions = make(map[task.BalanceEpochMeta]uint64)
	}
	clone.pendingEpochRevisions[revision.Epoch] = revision.EpochRevision
	return clone
}

func equalInt64Map(left, right map[int64]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, ok := right[key]
		if !ok || other != value {
			return false
		}
	}
	return true
}

type AdmissionToken struct {
	Snapshot           SnapshotToken
	Epoch              task.BalanceEpochMeta
	CollectionID       int64
	ReplicaID          int64
	ExpectedSourceNode int64
	Segment            *SegmentObjectKey
	Channel            *ChannelObjectKey
}

type NodeSnapshot struct {
	ID                int64
	Exists            bool
	Eligible          bool
	State             session.State
	ResourceGroup     string
	ResourceExhausted bool
	MemoryCapacity    float64
}

type ReplicaSnapshot struct {
	ID             int64
	CollectionID   int64
	ResourceGroup  string
	RWNodes        []int64
	RONodes        []int64
	RWSQNodes      []int64
	ROSQNodes      []int64
	ChannelRWNodes map[string][]int64
}

type SegmentPlacement struct {
	NodeID       int64
	CollectionID int64
	PartitionID  int64
	Channel      string
	RowCount     int64
	Version      int64
	Present      bool
}

type ChannelPlacement struct {
	NodeID              int64
	CollectionID        int64
	Version             int64
	Present             bool
	Serviceable         bool
	LeaderID            int64
	LeaderVersion       int64
	LeaderTargetVersion int64
	NumOfGrowingRows    int64
}

type TargetSegmentSnapshot struct {
	ID           int64
	CollectionID int64
	PartitionID  int64
	Channel      string
	RowCount     int64
}

type TargetChannelSnapshot struct {
	CollectionID      int64
	Channel           string
	GrowingSegmentIDs []int64
}

type TargetScopeSnapshot struct {
	Version  int64
	Segments map[int64]TargetSegmentSnapshot
	Channels map[string]TargetChannelSnapshot
}

type CollectionTargetSnapshot struct {
	Current TargetScopeSnapshot
	Next    TargetScopeSnapshot
}

type PendingWorkSnapshot struct {
	Revision              uint64
	Tasks                 []task.PendingBalanceTaskSnapshot
	SegmentWorkloadByNode map[int64]int
	ChannelWorkloadByNode map[int64]int
}

type PlacementSnapshot struct {
	Token             SnapshotToken
	CapturedAt        time.Time
	Nodes             map[int64]NodeSnapshot
	Replicas          map[int64]ReplicaSnapshot
	Segments          map[SegmentObjectKey][]SegmentPlacement
	Channels          map[ChannelObjectKey][]ChannelPlacement
	CollectionTargets map[int64]CollectionTargetSnapshot
	PendingWork       PendingWorkSnapshot
	EligibleReplicas  map[int64]struct{}
}
