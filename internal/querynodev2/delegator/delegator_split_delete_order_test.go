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
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// splitChildOf builds a real delegator for a split target and fronts it by
// s.delegator, so every delete it consumes is forwarded to the source exactly
// the way a spawned child forwards it.
func (s *DelegatorDataSuite) splitChildOf(vchannel string) *shardDelegator {
	child, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID, vchannel, s.version,
		s.workerManager, s.manager, s.loader, 10000, nil, s.chunkManager,
		NewChannelQueryView(nil, nil, nil, initialTargetVersion), nil)
	s.Require().NoError(err)
	sd := child.(*shardDelegator)
	// Close releases the child's function runners, which are process-global.
	s.T().Cleanup(sd.Close)
	sd.SetFrontingParent(s.delegator)
	return sd
}

// expectSealedBF makes every loaded sealed segment's bloom filter hold pks.
func (s *DelegatorDataSuite) expectSealedBF(pks ...int64) {
	s.loader.EXPECT().LoadBloomFilterSet(mock.Anything, s.collectionID, mock.Anything).
		Call.Return(func(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) []*pkoracle.BloomFilterSet {
		return lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) *pkoracle.BloomFilterSet {
			bfs := pkoracle.NewBloomFilterSet(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed)
			bf := bloomfilter.NewBloomFilterWithType(
				paramtable.Get().CommonCfg.BloomFilterSize.GetAsUint(),
				paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
				paramtable.Get().CommonCfg.BloomFilterType.GetValue())
			stats := &storage.PkStatistics{PkFilter: bf}
			stats.UpdatePKRange(&storage.Int64FieldData{Data: pks})
			bfs.AddHistoricalStats(stats)
			return bfs
		})
	}, func(ctx context.Context, collectionID int64, infos ...*querypb.SegmentLoadInfo) error {
		return nil
	})
}

func deleteOf(pk int64, ts uint64) []*DeleteData {
	return []*DeleteData{{
		PartitionID: 500,
		PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(pk)},
		Timestamps:  []uint64{ts},
		RowCount:    1,
	}}
}

// deletedPKsRecorder records every pk a worker.Delete forwards to the loading
// segment; onDelete, when set, runs inside the first call.
type deletedPKsRecorder struct {
	mu       sync.Mutex
	pks      map[int64]uint64
	calls    atomic.Int32
	onDelete func()
}

func (r *deletedPKsRecorder) record(req *querypb.DeleteRequest) {
	if r.calls.Add(1) == 1 && r.onDelete != nil {
		r.onDelete()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := req.GetPrimaryKeys().GetIntId().GetData()
	for i, id := range ids {
		r.pks[id] = req.GetTimestamps()[i]
	}
}

func (r *deletedPKsRecorder) has(pk int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pks[pk]
	return ok
}

func (s *DelegatorDataSuite) loadOneSealed(segmentID int64, startTs uint64, recorder *deletedPKsRecorder) {
	worker := &cluster.MockWorker{}
	worker.EXPECT().LoadSegments(mock.Anything, mock.AnythingOfType("*querypb.LoadSegmentsRequest")).Return(nil)
	worker.EXPECT().Delete(mock.Anything, mock.AnythingOfType("*querypb.DeleteRequest")).
		RunAndReturn(func(ctx context.Context, req *querypb.DeleteRequest) error {
			recorder.record(req)
			return nil
		})
	s.workerManager.EXPECT().GetWorker(mock.Anything, mock.AnythingOfType("int64")).Return(worker, nil)

	err := s.delegator.LoadSegments(context.Background(), &querypb.LoadSegmentsRequest{
		Base:         commonpbutil.NewMsgBase(),
		DstNodeID:    1,
		CollectionID: s.collectionID,
		Infos: []*querypb.SegmentLoadInfo{{
			SegmentID:     segmentID,
			PartitionID:   500,
			StartPosition: &msgpb.MsgPosition{Timestamp: startTs},
			DeltaPosition: &msgpb.MsgPosition{Timestamp: startTs},
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", s.collectionID),
		}},
	})
	s.Require().NoError(err)
}

// Two split children consume different WALs, so the deletes they forward reach
// the source's delete buffer in no timestamp order. A segment the source loads
// afterwards must still receive every buffered delete at or after its start:
// with the buffer's binary search over an unsorted block, the delete at 1010
// (child 1) is skipped because child 2's 990 sits between it and 1020.
func (s *DelegatorDataSuite) TestSplitSourceLoadAppliesChildDeletesForwardedOutOfOrder() {
	defer func() {
		s.workerManager.ExpectedCalls = nil
		s.loader.ExpectedCalls = nil
	}()
	s.expectSealedBF(10, 11, 12)

	child1 := s.splitChildOf(fmt.Sprintf("by-dev-rootcoord-dml_1_%dv1", s.collectionID))
	child2 := s.splitChildOf(fmt.Sprintf("by-dev-rootcoord-dml_2_%dv2", s.collectionID))
	child1.ProcessDelete(deleteOf(10, 1010), 1010)
	child2.ProcessDelete(deleteOf(11, 990), 990)
	child1.ProcessDelete(deleteOf(12, 1020), 1020)

	recorder := &deletedPKsRecorder{pks: make(map[int64]uint64)}
	s.loadOneSealed(401, 1001, recorder)

	s.True(recorder.has(10), "the delete at 1010, forwarded by child 1 before child 2's 990, was never applied to the loaded segment")
	s.True(recorder.has(12))
}

// A delete forwarded while the source is part-way through a load can carry a
// timestamp BELOW the newest one its Phase-1 snapshot saw, because the other
// child forwarded that newer one first. The catch-up must still apply it: a
// cursor at snapshotMaxTs+1 never looks back at it.
func (s *DelegatorDataSuite) TestSplitSourceLoadCatchesUpAChildDeleteOlderThanItsSnapshot() {
	defer func() {
		s.workerManager.ExpectedCalls = nil
		s.loader.ExpectedCalls = nil
	}()
	s.expectSealedBF(20, 21)

	child1 := s.splitChildOf(fmt.Sprintf("by-dev-rootcoord-dml_1_%dv1", s.collectionID))
	child2 := s.splitChildOf(fmt.Sprintf("by-dev-rootcoord-dml_2_%dv2", s.collectionID))
	// child 2 is ahead: its 1030 is in the Phase-1 snapshot.
	child2.ProcessDelete(deleteOf(20, 1030), 1030)

	recorder := &deletedPKsRecorder{pks: make(map[int64]uint64)}
	// Phase 2's bulk flush runs lock-free; child 1 forwards its older delete
	// then, after the snapshot was taken.
	recorder.onDelete = func() { child1.ProcessDelete(deleteOf(21, 1015), 1015) }
	s.loadOneSealed(402, 1001, recorder)

	s.True(recorder.has(20))
	s.True(recorder.has(21), "the delete at 1015, forwarded during the load but older than the snapshot's 1030, was never caught up")
}

// registerL0 gives d a loaded L0 segment holding one delete per (pk, ts).
func (s *DelegatorDataSuite) registerL0(d *shardDelegator, segmentID int64, deletes map[int64]uint64) {
	l0, err := segments.NewL0Segment(d.collection, segments.SegmentTypeSealed, s.version, &querypb.SegmentLoadInfo{
		CollectionID:  s.collectionID,
		SegmentID:     segmentID,
		PartitionID:   500,
		InsertChannel: d.vchannelName,
		Level:         datapb.SegmentLevel_L0,
		StartPosition: &msgpb.MsgPosition{Timestamp: 1001},
	})
	s.Require().NoError(err)
	deltaData := storage.NewDeltaData(int64(len(deletes)))
	for pk, ts := range deletes {
		s.Require().NoError(deltaData.Append(storage.NewInt64PrimaryKey(pk), ts))
	}
	s.Require().NoError(l0.LoadDeltaData(context.Background(), deltaData))
	d.deleteBuffer.RegisterL0(l0)
}

// A child built from its target's recovery view -- a respawn after a
// QueryNode restart mid-window -- consumes its WAL only from the target's
// checkpoint. The target's deletes in (T_switch, checkpoint] are in the
// target's L0 segments, which only the child loads; they must still reach the
// source's view, both the segments it already serves and those it loads later.
func (s *DelegatorDataSuite) TestSplitChildForwardsItsL0AndBufferedDeletesToTheSource() {
	defer func() {
		s.workerManager.ExpectedCalls = nil
		s.loader.ExpectedCalls = nil
	}()
	s.expectSealedBF(30, 31, 32)

	child, err := NewShardDelegator(context.Background(), s.collectionID, s.replicaID,
		fmt.Sprintf("by-dev-rootcoord-dml_1_%dv1", s.collectionID), s.version,
		s.workerManager, s.manager, s.loader, 10000, nil, s.chunkManager,
		NewChannelQueryView(nil, nil, nil, initialTargetVersion), nil)
	s.Require().NoError(err)
	sd := child.(*shardDelegator)
	s.T().Cleanup(sd.Close)
	// deletes the child already holds before it is fronted: its target's L0
	// (below the checkpoint) and one it consumed into its buffer.
	s.registerL0(sd, 900, map[int64]uint64{30: 1005, 31: 1008})
	sd.ProcessDelete(deleteOf(32, 1012), 1012)

	// nothing is forwarded without a fronting parent.
	s.NoError(sd.ForwardKnownDeletesToParent(context.Background()))

	sd.SetFrontingParent(s.delegator)
	s.NoError(sd.ForwardKnownDeletesToParent(context.Background()))

	recorder := &deletedPKsRecorder{pks: make(map[int64]uint64)}
	s.loadOneSealed(403, 1001, recorder)
	s.True(recorder.has(30), "the target L0 delete at 1005 never reached a segment the source loads")
	s.True(recorder.has(31), "the target L0 delete at 1008 never reached a segment the source loads")
	s.True(recorder.has(32), "the child's buffered delete never reached a segment the source loads")
}

// Under the RemoteLoad L0 policy a delegator's L0 segments carry no records in
// memory, so the forward loads them itself and releases them afterwards.
func (s *DelegatorDataSuite) TestSplitChildForwardsRemoteLoadL0Deletes() {
	defer func() {
		s.loader.ExpectedCalls = nil
	}()
	child := s.splitChildOf(fmt.Sprintf("by-dev-rootcoord-dml_1_%dv1", s.collectionID))
	child.l0ForwardPolicy = L0ForwardPolicyRemoteLoad
	remote, err := segments.NewL0Segment(child.collection, segments.SegmentTypeSealed, s.version, &querypb.SegmentLoadInfo{
		CollectionID: s.collectionID, SegmentID: 901, PartitionID: 500, Level: datapb.SegmentLevel_L0,
		InsertChannel: child.vchannelName,
	})
	s.Require().NoError(err)
	child.deleteBuffer.RegisterL0(remote)

	loaded, err := segments.NewL0Segment(child.collection, segments.SegmentTypeSealed, s.version, remote.LoadInfo())
	s.Require().NoError(err)
	deltaData := storage.NewDeltaData(1)
	s.Require().NoError(deltaData.Append(storage.NewInt64PrimaryKey(40), 1030))
	s.Require().NoError(loaded.(*segments.L0Segment).LoadDeltaData(context.Background(), deltaData))
	s.loader.EXPECT().Load(mock.Anything, s.collectionID, segments.SegmentTypeSealed, mock.Anything, mock.Anything).
		Return([]segments.Segment{loaded}, nil).Once()

	s.NoError(child.ForwardKnownDeletesToParent(context.Background()))
	items := s.delegator.deleteBuffer.ListAfter(1030)
	s.Require().Len(items, 1)
	s.Equal(storage.NewInt64PrimaryKey(40).GetValue(), items[0].Data[0].DeleteData.Pks[0].GetValue())
	pks, _ := loaded.(*segments.L0Segment).DeleteRecords()
	s.Empty(pks, "the L0 loaded for the forward is released")

	// a failed load fails the forward.
	s.loader.EXPECT().Load(mock.Anything, s.collectionID, segments.SegmentTypeSealed, mock.Anything, mock.Anything).
		Return(nil, errors.New("mock load failure")).Once()
	s.Error(child.ForwardKnownDeletesToParent(context.Background()))
}
