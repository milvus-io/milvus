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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSegmentChangeRegistrationLocksOperatorSources(t *testing.T) {
	for _, producer := range []bool{false, true} {
		name := "composite"
		if producer {
			name = "producer"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			member := flushedTestSegment(2, 10)
			member.IsInvisible = true
			m, _ := newSegmentLockTestMeta(t, flushedTestSegment(1, 10), member)
			unlock := sync.OnceFunc(m.LockSegments(1))
			defer unlock()
			g := &model.SegmentChangeGroup{
				GroupID: 100, SourceJobID: 100, CollectionID: 100,
				Source: model.SegmentChangeSourceCopySegment, NewSegmentIDs: []int64{2},
			}
			derive := func(pack *UpdateSegmentPack) bool {
				source := pack.Peek(1) // A source outside the group's membership.
				if source == nil {
					return false // A contended smaller ID makes the pack rerun.
				}
				pack.Get(2).MaxRowNum = source.GetMaxRowNum() + 1
				return true
			}
			done := make(chan error, 1)
			go func() {
				if producer {
					_, _, err := m.StageAdmittedChange(ctx, g, true, derive)
					done <- err
				} else {
					done <- m.UpdateSegmentsInfoAndChangeGroups(ctx, []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)},
						derive, SetSegmentIsInvisible(2, true))
				}
			}()
			select {
			case err := <-done:
				t.Fatalf("registration did not wait for its source writer: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			source := m.GetSegment(ctx, 1).Clone()
			source.MaxRowNum = 10
			m.Segments.SetSegment(1, source)
			unlock()
			waitDone(t, done, "group registration")
			require.EqualValues(t, 11, m.GetSegment(ctx, 2).GetMaxRowNum())
			require.True(t, m.GetSegment(ctx, 2).GetIsInvisible())
			require.NotNil(t, m.GetSegmentChangeGroup(ctx, 100, 100))
		})
	}
}

func TestSegmentChangeRegistrationWaitsForMemberWrite(t *testing.T) {
	ctx := context.Background()
	member := flushedTestSegment(1, 10)
	member.IsInvisible = true
	m, catalog := newSegmentLockTestMeta(t, member)
	gate := catalog.gate(1)
	open := sync.OnceFunc(func() { close(gate) })
	defer open()
	written := make(chan error, 1)
	go func() {
		written <- m.UpdateSegmentsInfo(ctx, func(pack *UpdateSegmentPack) bool {
			pack.Get(1).MaxRowNum = 10
			return true
		})
	}()
	select {
	case <-catalog.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("member writer did not reach the catalog")
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := m.StageAdmittedChange(ctx, &model.SegmentChangeGroup{
			GroupID: 100, SourceJobID: 100, CollectionID: 100,
			Source: model.SegmentChangeSourceCopySegment, NewSegmentIDs: []int64{1},
		}, true)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("registration did not wait for its member writer: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	open()
	waitDone(t, written, "member write")
	waitDone(t, done, "group registration")
	require.EqualValues(t, 10, m.GetSegment(ctx, 1).GetMaxRowNum())
	require.True(t, m.GetSegment(ctx, 1).GetIsInvisible())
}

func TestSegmentChangeCompactionUsesHeldWriterLocks(t *testing.T) {
	ctx := context.Background()
	m, _ := newSegmentLockTestMeta(t, flushedTestSegment(1, 10))
	schema := NewTestSchema()
	m.AddCollection(&CollectionInfo{ID: 100, Schema: schema})
	key := Params.DataCoordCfg.SegmentChangeEnabled.Key
	old := Params.DataCoordCfg.SegmentChangeEnabled.GetValue()
	require.NoError(t, Params.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, Params.Save(key, old)) })
	done := make(chan error, 1)
	go func() {
		for i, kind := range []datapb.CompactionType{datapb.CompactionType_MixCompaction, datapb.CompactionType_SortCompaction} {
			_, err := m.CompleteCompactionMutation(ctx, &datapb.CompactionTask{
				PlanID: int64(100 + i), CollectionID: 100, PartitionID: 10,
				Type: kind, InputSegments: []int64{int64(1 + i)}, Schema: schema,
			}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
				SegmentID: int64(2 + i), NumOfRows: 1, IsSorted: i > 0,
				InsertLogs: flushedTestSegment(int64(2+i), 10).GetBinlogs(),
			}}})
			if err != nil {
				done <- err
				return
			}
		}
		g := m.GetSegmentChangeGroup(ctx, 100, 100)
		g.State = model.SegmentChangeStateAborted
		if err := m.UpdateSegmentChangeGroup(ctx, g); err != nil {
			done <- err
			return
		}
		done <- m.ReclaimSegmentChangeGroup(ctx, g)
	}()
	waitDone(t, done, "staged mix, private sort and reclamation")
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 1).GetState())
	for _, id := range []int64{2, 3} {
		require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, id).GetState())
	}
}
