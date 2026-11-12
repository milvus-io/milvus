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

package compaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestCompactionCandidateAndSelectability(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*meta.Meta, *meta.SegmentInfo)
		wantCandidate bool
		wantShared    bool
		wantMix       bool
	}{
		{
			name:          "flushed sorted L1 segment",
			wantCandidate: true,
			wantShared:    true,
			wantMix:       true,
		},
		{
			name: "flushing segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Flushing
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "growing segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Growing
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "dropped segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.State = commonpb.SegmentState_Dropped
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "importing segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsImporting = true
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "L0 segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L0
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "L2 segment is not a normal manual candidate",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.Level = datapb.SegmentLevel_L2
			},
			wantShared: true,
			wantMix:    true,
		},
		{
			name: "snapshot protection is a shared selectability blocker",
			mutate: func(mt *meta.Meta, segment *meta.SegmentInfo) {
				snapMeta := &snapshot.SnapshotMeta{
					SegmentProtectionUntil: map[int64]uint64{
						segment.GetID(): uint64(time.Now().Add(time.Hour).Unix()),
					},
				}
				mt.CompactionProtection = snapMeta
			},
			wantMix: true,
		},
		{
			name: "compacting is a shared selectability blocker",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsCompacting = true
			},
			wantMix: true,
		},
		{
			name: "invisible is a mix selectability blocker",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsInvisible = true
			},
			wantShared: true,
		},
		{
			name: "unsorted is a mix selectability blocker",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsSorted = false
			},
			wantShared: true,
		},
		{
			name: "namespace sorted segment is executable",
			mutate: func(_ *meta.Meta, segment *meta.SegmentInfo) {
				segment.IsSorted = false
				segment.IsSortedByNamespace = true
			},
			wantCandidate: true,
			wantShared:    true,
			wantMix:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mt := &meta.Meta{}
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:          1,
				State:       commonpb.SegmentState_Flushed,
				Level:       datapb.SegmentLevel_L1,
				IsSorted:    true,
				IsImporting: false,
			}}
			if test.mutate != nil {
				test.mutate(mt, segment)
			}

			require.Equal(t, test.wantCandidate, policy.IsNormalManualCompactionCandidate(mt, segment))
			require.Equal(t, test.wantShared, policy.IsSharedCompactionSelectable(mt, segment))
			require.Equal(t, test.wantMix, policy.IsMixCompactionSelectable(segment))
		})
	}
}
