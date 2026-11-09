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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/segment"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLastExpireReset(t *testing.T) {
	dm := &domainMetas{}

	// set up meta on dc
	ctx := context.Background()
	paramtable.Init()
	Params.Save(Params.DataCoordCfg.AllocLatestExpireAttempt.Key, "1")
	Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "1")
	defer func() {
		Params.Save(Params.DataCoordCfg.AllocLatestExpireAttempt.Key, "200")
		Params.Save(Params.DataCoordCfg.SegmentMaxSize.Key, "1024")
	}()
	mockAllocator := allocator.NewRootCoordAllocator(meta.NewMockMixCoord())
	etcdCli, _ := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	rootPath := "/test/segment/last/expire"
	metaKV := etcdkv.NewEtcdKV(etcdCli, rootPath)
	metaKV.RemoveWithPrefix(ctx, "")
	catalog := datacoord.NewCatalog(metaKV, "", "")
	collID, err := mockAllocator.AllocID(ctx)
	assert.Nil(t, err)
	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{
			{
				DbName:        "db_1",
				CollectionIDs: []int64{collID},
			},
		},
	}, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm))
	assert.Nil(t, err)
	// add collection
	channelName := "c1"
	schema := meta.NewTestSchema()
	mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	initSegment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  collID,
			InsertChannel: "ch1",
			State:         commonpb.SegmentState_Growing,
		},
	}
	mt.AddSegment(context.TODO(), initSegment)

	// assign segments, set max segment to only 1MB, equalling to 10485 rows
	var bigRows, smallRows int64 = 10000, 1000
	segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator)
	initSegment.State = commonpb.SegmentState_Dropped
	mt.Segments.SetSegment(1, initSegment)
	allocs, _ := segmentManager.AllocSegment(context.Background(), collID, 0, channelName, bigRows, storage.StorageV1)
	segmentID1, expire1 := allocs[0].SegmentID, allocs[0].ExpireTime
	time.Sleep(100 * time.Millisecond)
	allocs, _ = segmentManager.AllocSegment(context.Background(), collID, 0, channelName, bigRows, storage.StorageV1)
	segmentID2, expire2 := allocs[0].SegmentID, allocs[0].ExpireTime
	time.Sleep(100 * time.Millisecond)
	allocs, _ = segmentManager.AllocSegment(context.Background(), collID, 0, channelName, smallRows, storage.StorageV1)
	segmentID3, expire3 := allocs[0].SegmentID, allocs[0].ExpireTime

	// simulate handleTimeTick op on dataCoord
	// meta.SetLastWrittenTime(segmentID1, bigRows)
	// meta.SetLastWrittenTime(segmentID2, bigRows)
	// meta.SetLastWrittenTime(segmentID3, smallRows)
	mt.SetRowCount(segmentID1, bigRows)
	mt.SetRowCount(segmentID2, bigRows)
	mt.SetRowCount(segmentID3, smallRows)
	err = segmentManager.TryToSealSegment(context.TODO(), expire1, channelName)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.SegmentState_Sealed, mt.GetSegment(context.TODO(), segmentID1).GetState())
	assert.Equal(t, commonpb.SegmentState_Sealed, mt.GetSegment(context.TODO(), segmentID2).GetState())
	assert.Equal(t, commonpb.SegmentState_Growing, mt.GetSegment(context.TODO(), segmentID3).GetState())

	// pretend that dataCoord break down
	metaKV.Close()
	etcdCli.Close()

	// dataCoord restart
	newEtcdCli, _ := etcd.GetEtcdClient(Params.EtcdCfg.UseEmbedEtcd.GetAsBool(), Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(), Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(), Params.EtcdCfg.EtcdTLSCACert.GetValue(), Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	newMetaKV := etcdkv.NewEtcdKV(newEtcdCli, rootPath)
	defer newMetaKV.RemoveWithPrefix(ctx, "")
	newCatalog := datacoord.NewCatalog(newMetaKV, "", "")
	restartedMeta, err := meta.NewMeta(context.TODO(), newCatalog, nil, broker, loadDomainMetas(newCatalog, nil, dm))
	restartedMeta.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
	assert.Nil(t, err)
	newSegmentManager, _ := segment.NewSegmentManager(restartedMeta, mockAllocator)
	// reset row number to avoid being cleaned by empty segment
	restartedMeta.SetRowCount(segmentID1, bigRows)
	restartedMeta.SetRowCount(segmentID2, bigRows)
	restartedMeta.SetRowCount(segmentID3, smallRows)

	// verify lastExpire of growing and sealed segments
	segment1, segment2, segment3 := restartedMeta.GetSegment(context.TODO(), segmentID1), restartedMeta.GetSegment(context.TODO(), segmentID2), restartedMeta.GetSegment(context.TODO(), segmentID3)
	// segmentState should not be altered but growing segment's lastExpire has been reset to the latest
	assert.Equal(t, commonpb.SegmentState_Sealed, segment1.GetState())
	assert.Equal(t, commonpb.SegmentState_Sealed, segment2.GetState())
	assert.Equal(t, commonpb.SegmentState_Growing, segment3.GetState())
	assert.Equal(t, expire1, segment1.GetLastExpireTime())
	assert.Equal(t, expire2, segment2.GetLastExpireTime())
	assert.True(t, segment3.GetLastExpireTime() > expire3)
	flushableSegIDs, _ := newSegmentManager.GetFlushableSegments(context.Background(), channelName, expire3)
	assert.ElementsMatch(t, []UniqueID{segmentID1, segmentID2}, flushableSegIDs) // segment1 and segment2 can be flushed
	newAlloc, err := newSegmentManager.AllocSegment(context.Background(), collID, 0, channelName, 2000, storage.StorageV1)
	assert.Nil(t, err)
	assert.Equal(t, segmentID3, newAlloc[0].SegmentID) // segment3 still can be used to allocate
}

func alwaysSealPolicy() segment.SegmentSealPolicyFunc {
	return func(seg *meta.SegmentInfo, ts Timestamp) (bool, string) {
		return true, ""
	}
}

func TestTryToSealSegment(t *testing.T) {
	dm := &domainMetas{}

	paramtable.Init()
	pt := paramtable.Get()
	t.Run("normal_seal with_segment_policies", func(t *testing.T) {
		pt.Save(pt.DataCoordCfg.SegmentMaxLifetime.Key, fmt.Sprintf("%d", math.MinInt64))
		defer pt.Reset(pt.DataCoordCfg.SegmentMaxLifetime.Key)
		mockAllocator := meta.NewMockAllocator(t)
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator, segment.WithSegmentSealPolices(segment.SealL1SegmentByLifetime())) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.Meta.Segments.Segments {
			if seg.GetStartPosition() != nil {
				assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
			}
		}
	})

	t.Run("normal_seal_with_channel_seal_policies", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := meta.NewMockAllocator(t)
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator, segment.WithChannelSealPolices(segment.GetChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.Meta.Segments.Segments {
			assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
		}
	})

	t.Run("normal seal with both segment & channel seal policy", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := meta.NewMockAllocator(t)
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator,
			segment.WithSegmentSealPolices(segment.SealL1SegmentByLifetime()),
			segment.WithChannelSealPolices(segment.GetChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
		assert.NoError(t, err)

		for _, seg := range segmentManager.Meta.Segments.Segments {
			assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
		}
	})

	t.Run("test sealByMaxBinlogFileNumberPolicy", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := meta.NewMockAllocator(t)
		mt, err := meta.NewMemoryMeta(t)
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator)
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)

		// No seal polices
		{
			err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
			assert.NoError(t, err)
			segments := segmentManager.Meta.Segments.Segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				assert.Equal(t, commonpb.SegmentState_Growing, seg.GetState())
			}
		}

		// Not trigger seal
		{
			segmentManager.SegmentSealPolicies = []segment.SegmentSealPolicy{segment.SealL1SegmentByLifetime()}
			segments := segmentManager.Meta.Segments.Segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				seg.Statslogs = []*datapb.FieldBinlog{
					{
						FieldID: 2,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum: 10,
								LogID:      3,
							},
						},
					},
				}
				err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
				assert.NoError(t, err)
				seg = segmentManager.Meta.Segments.Segments[seg.ID]
				assert.Equal(t, commonpb.SegmentState_Growing, seg.GetState())
			}
		}

		// Trigger seal
		{
			segmentManager.SegmentSealPolicies = []segment.SegmentSealPolicy{segment.SealL1SegmentByBinlogFileNumber(2)}
			segments := segmentManager.Meta.Segments.Segments
			assert.Equal(t, 1, len(segments))
			for _, seg := range segments {
				seg.Binlogs = []*datapb.FieldBinlog{
					{
						FieldID: 1,
						Binlogs: []*datapb.Binlog{
							{
								EntriesNum: 10,
								LogID:      1,
							},
							{
								EntriesNum: 20,
								LogID:      2,
							},
						},
					},
				}
				err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
				assert.NoError(t, err)
				seg = segmentManager.Meta.Segments.Segments[seg.ID]
				assert.Equal(t, commonpb.SegmentState_Sealed, seg.GetState())
			}
		}
	})

	t.Run("seal with segment policy with kv fails", func(t *testing.T) {
		mockAllocator := meta.NewMockAllocator(t)
		memoryKV := meta.NewMetaMemoryKV()
		catalog := datacoord.NewCatalog(memoryKV, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm))
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator, segment.WithSegmentSealPolices(alwaysSealPolicy())) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		segmentManager.Meta.Catalog = &datacoord.Catalog{MetaKv: metakv}

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
		assert.Error(t, err)
	})

	t.Run("seal with channel policy with kv fails", func(t *testing.T) {
		paramtable.Init()
		mockAllocator := meta.NewMockAllocator(t)
		memoryKV := meta.NewMetaMemoryKV()
		catalog := datacoord.NewCatalog(memoryKV, "", "")
		broker := broker.NewMockBroker(t)
		broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
		mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm))
		assert.NoError(t, err)

		schema := meta.NewTestSchema()
		collID, err := mockAllocator.AllocID(context.Background())
		assert.NoError(t, err)
		mt.AddCollection(&meta.CollectionInfo{ID: collID, Schema: schema})
		segmentManager, _ := segment.NewSegmentManager(mt, mockAllocator, segment.WithChannelSealPolices(segment.GetChannelOpenSegCapacityPolicy(-1))) // always seal
		allocations, err := segmentManager.AllocSegment(context.TODO(), collID, 0, "c1", 2, storage.StorageV1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(allocations))

		metakv := mockkv.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
		metakv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
		segmentManager.Meta.Catalog = &datacoord.Catalog{MetaKv: metakv}

		ts, err := segmentManager.Allocator.AllocTimestamp(context.Background())
		assert.NoError(t, err)
		err = segmentManager.TryToSealSegment(context.TODO(), ts, "c1")
		assert.Error(t, err)
	})
}
