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

package datacoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// ErrSplitPlannerNotReady is returned by the placeholder planner; a task
// stays in the Preparing state (still abortable) until a real planner is
// wired in.
var ErrSplitPlannerNotReady = errors.New("shard split planner is not ready")

// splitPlanner decides the split point of a shard and assigns segments to
// the target shards. The production implementation works on the range
// routing key space (big_endian(hash(namespace)) || namespace): it picks a
// namespace boundary balancing the two halves as the split point, and maps
// a segment to the target whose key range contains its namespace.
type splitPlanner interface {
	// PlanTargets selects the split point of the source shard and returns
	// the target shards with their routing key ranges. The ranges must be
	// disjoint and exactly cover the source shard's range.
	PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error)
	// AssignSegment returns the index of the target shard owning the
	// segment (decided by the namespace of the segment's partition).
	AssignSegment(segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error)
}

// unimplementedSplitPlanner keeps every split task in the Preparing state.
// It is the default until the range-routing planner lands.
type unimplementedSplitPlanner struct{}

func (unimplementedSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error) {
	return nil, ErrSplitPlannerNotReady
}

func (unimplementedSplitPlanner) AssignSegment(segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	return 0, ErrSplitPlannerNotReady
}
