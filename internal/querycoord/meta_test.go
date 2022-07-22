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

package querycoord

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

func successResult() error { return nil }
func failedResult() error  { return errors.New("") }

type testKv struct {
	kv.MetaKv
	returnFn func() error
}

func (tk *testKv) Save(key, value string) error {
	return tk.returnFn()
}

func (tk *testKv) MultiSave(saves map[string]string) error {
	return tk.returnFn()
}

func (tk *testKv) Remove(key string) error {
	return tk.returnFn()
}

func (tk *testKv) LoadWithPrefix(key string) ([]string, []string, error) {
	return nil, nil, nil
}

func (tk *testKv) Load(key string) (string, error) {
	return "", nil
}

func TestReplica_Release(t *testing.T) {
	refreshParams()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta, err := newMeta(context.Background(), etcdKV, nil, idAllocator)
	assert.Nil(t, err)
	err = meta.addCollection(1, querypb.LoadType_LoadCollection, nil)
	require.NoError(t, err)

	collections := meta.showCollections()
	assert.Equal(t, 1, len(collections))

	err = meta.addPartitions(1, []UniqueID{100})
	assert.NoError(t, err)
	partitions, err := meta.showPartitions(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))

	meta.releasePartitions(1, []UniqueID{100})
	partitions, err = meta.showPartitions(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(partitions))
	meta.releasePartitions(1, []UniqueID{100})

	meta.releaseCollection(1)
	collections = meta.showCollections()
	assert.Equal(t, 0, len(collections))
	meta.releaseCollection(1)
}

func TestMetaFunc(t *testing.T) {
	refreshParams()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	nodeID := defaultQueryNodeID
	segmentsInfo := newSegmentsInfo(kv)
	segmentsInfo.segmentIDMap[defaultSegmentID] = &querypb.SegmentInfo{
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		SegmentID:    defaultSegmentID,
		NodeID:       nodeID,
		NodeIds:      []int64{nodeID},
	}
	meta := &MetaReplica{
		collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
		dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
		segmentsInfo:    segmentsInfo,
		replicas:        NewReplicaInfos(),
	}
	meta.setKvClient(kv)
	dmChannels := []string{"testDm1", "testDm2"}

	t.Run("Test ShowPartitionFail", func(t *testing.T) {
		res, err := meta.showPartitions(defaultCollectionID)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test HasCollectionFalse", func(t *testing.T) {
		hasCollection := meta.hasCollection(defaultCollectionID)
		assert.Equal(t, false, hasCollection)
	})

	t.Run("Test HasPartitionFalse", func(t *testing.T) {
		hasPartition := meta.hasPartition(defaultCollectionID, defaultPartitionID)
		assert.Equal(t, false, hasPartition)
	})

	t.Run("Test HasReleasePartitionFalse", func(t *testing.T) {
		hasReleasePartition := meta.hasReleasePartition(defaultCollectionID, defaultPartitionID)
		assert.Equal(t, false, hasReleasePartition)
	})

	t.Run("Test GetSegmentInfoByIDFail", func(t *testing.T) {
		res, err := meta.getSegmentInfoByID(defaultSegmentID + 100)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test GetCollectionInfoByIDFail", func(t *testing.T) {
		res, err := meta.getCollectionInfoByID(defaultCollectionID)
		assert.Nil(t, res)
		assert.NotNil(t, err)
	})

	t.Run("Test GetPartitionStatesByIDFail", func(t *testing.T) {
		res, err := meta.getPartitionStatesByID(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, res)
		assert.NotNil(t, err)
	})

	t.Run("Test SetLoadTypeFail", func(t *testing.T) {
		err := meta.setLoadType(defaultCollectionID, querypb.LoadType_LoadCollection)
		assert.NotNil(t, err)
	})

	t.Run("Test SetLoadPercentageFail", func(t *testing.T) {
		err := meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_LoadCollection)
		assert.NotNil(t, err)
	})

	t.Run("Test AddCollection", func(t *testing.T) {
		schema := genDefaultCollectionSchema(false)
		err := meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, schema)
		assert.Nil(t, err)
	})

	t.Run("Test HasCollection", func(t *testing.T) {
		hasCollection := meta.hasCollection(defaultCollectionID)
		assert.Equal(t, true, hasCollection)
	})

	t.Run("Test AddPartition", func(t *testing.T) {
		err := meta.addPartitions(defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Nil(t, err)
	})

	t.Run("Test HasPartition", func(t *testing.T) {
		hasPartition := meta.hasPartition(defaultCollectionID, defaultPartitionID)
		assert.Equal(t, true, hasPartition)
	})

	t.Run("Test ShowCollections", func(t *testing.T) {
		info := meta.showCollections()
		assert.Equal(t, 1, len(info))
	})

	t.Run("Test ShowPartitions", func(t *testing.T) {
		states, err := meta.showPartitions(defaultCollectionID)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(states))
	})

	t.Run("Test GetCollectionInfoByID", func(t *testing.T) {
		info, err := meta.getCollectionInfoByID(defaultCollectionID)
		assert.Nil(t, err)
		assert.Equal(t, defaultCollectionID, info.CollectionID)
	})

	t.Run("Test GetPartitionStatesByID", func(t *testing.T) {
		state, err := meta.getPartitionStatesByID(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, err)
		assert.Equal(t, defaultPartitionID, state.PartitionID)
	})

	t.Run("Test AddDmChannel", func(t *testing.T) {
		var dmChannelWatchInfos []*querypb.DmChannelWatchInfo
		for _, channel := range dmChannels {
			dmChannelWatchInfos = append(dmChannelWatchInfos, &querypb.DmChannelWatchInfo{
				CollectionID: defaultCollectionID,
				DmChannel:    channel,
				NodeIDLoaded: nodeID,
				NodeIds:      []int64{nodeID},
			})
		}
		err = meta.setDmChannelInfos(dmChannelWatchInfos...)
		assert.Nil(t, err)
	})

	t.Run("Test GetDmChannelsByNodeID", func(t *testing.T) {
		channelInfos := meta.getDmChannelInfosByNodeID(nodeID)
		assert.Equal(t, 2, len(channelInfos))
	})

	t.Run("Test ShowSegmentInfo", func(t *testing.T) {
		infos := meta.showSegmentInfos(defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Equal(t, 1, len(infos))
		assert.Equal(t, defaultSegmentID, infos[0].SegmentID)
	})

	t.Run("Test GetSegmentInfoByNode", func(t *testing.T) {
		infos := meta.getSegmentInfosByNode(nodeID)
		assert.Equal(t, 1, len(infos))
		assert.Equal(t, defaultSegmentID, infos[0].SegmentID)
	})

	t.Run("Test GetSegmentInfoByID", func(t *testing.T) {
		info, err := meta.getSegmentInfoByID(defaultSegmentID)
		assert.Nil(t, err)
		assert.Equal(t, defaultSegmentID, info.SegmentID)
	})

	t.Run("Test SetLoadType", func(t *testing.T) {
		err := meta.setLoadType(defaultCollectionID, querypb.LoadType_LoadCollection)
		assert.Nil(t, err)
	})

	t.Run("Test SetLoadPercentage", func(t *testing.T) {
		err := meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_LoadPartition)
		assert.Nil(t, err)
		state, err := meta.getPartitionStatesByID(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, err)
		assert.Equal(t, int64(100), state.InMemoryPercentage)
		err = meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_LoadCollection)
		assert.Nil(t, err)
		info, err := meta.getCollectionInfoByID(defaultCollectionID)
		assert.Nil(t, err)
		assert.Equal(t, int64(100), info.InMemoryPercentage)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		err := meta.releasePartitions(defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		err := meta.releaseCollection(defaultCollectionID)
		assert.Nil(t, err)
	})
}

func TestReloadMetaFromKV(t *testing.T) {
	refreshParams()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta := &MetaReplica{
		idAllocator:       idAllocator,
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		dmChannelInfos:    map[string]*querypb.DmChannelWatchInfo{},
		deltaChannelInfos: map[UniqueID][]*datapb.VchannelInfo{},
		segmentsInfo:      newSegmentsInfo(kv),
		replicas:          NewReplicaInfos(),
	}
	meta.setKvClient(kv)

	kvs := make(map[string]string)
	collectionInfo := &querypb.CollectionInfo{
		CollectionID: defaultCollectionID,
	}
	collectionBlobs, err := proto.Marshal(collectionInfo)
	assert.Nil(t, err)
	collectionKey := fmt.Sprintf("%s/%d", collectionMetaPrefix, defaultCollectionID)
	kvs[collectionKey] = string(collectionBlobs)

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    defaultSegmentID,
		CollectionID: defaultCollectionID,
	}
	segmentBlobs, err := proto.Marshal(segmentInfo)
	assert.Nil(t, err)
	segmentKey := fmt.Sprintf("%s/%d", util.SegmentMetaPrefix, defaultSegmentID)
	kvs[segmentKey] = string(segmentBlobs)

	deltaChannel1 := &datapb.VchannelInfo{CollectionID: defaultCollectionID, ChannelName: "delta-channel1"}
	deltaChannel2 := &datapb.VchannelInfo{CollectionID: defaultCollectionID, ChannelName: "delta-channel2"}

	infos := []*datapb.VchannelInfo{deltaChannel1, deltaChannel2}
	for _, info := range infos {
		infoBytes, err := proto.Marshal(info)
		assert.Nil(t, err)

		key := fmt.Sprintf("%s/%d/%s", deltaChannelMetaPrefix, defaultCollectionID, info.ChannelName)
		kvs[key] = string(infoBytes)
	}

	dmChannelInfo := &querypb.DmChannelWatchInfo{
		CollectionID: defaultCollectionID,
		DmChannel:    "dm-channel1",
	}
	dmChannelInfoBlob, err := proto.Marshal(dmChannelInfo)
	assert.Nil(t, err)
	key := fmt.Sprintf("%s/%d/%s", dmChannelMetaPrefix, dmChannelInfo.CollectionID, dmChannelInfo.DmChannel)
	kvs[key] = string(dmChannelInfoBlob)

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 1, len(meta.collectionInfos))
	assert.Equal(t, 1, len(meta.segmentsInfo.getSegments()))
	collectionInfo, err = meta.getCollectionInfoByID(collectionInfo.CollectionID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(collectionInfo.ReplicaIds))
	assert.Equal(t, int32(1), collectionInfo.ReplicaNumber)
	segment := meta.segmentsInfo.getSegment(defaultSegmentID)
	assert.NotNil(t, segment)

	replicas, err := meta.getReplicasByCollectionID(collectionInfo.CollectionID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(replicas))
	assert.Equal(t, collectionInfo.CollectionID, replicas[0].CollectionID)
}

func TestVChannelInfoReadFromKVCompatible(t *testing.T) {
	refreshParams()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta := &MetaReplica{
		idAllocator:       idAllocator,
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		dmChannelInfos:    map[string]*querypb.DmChannelWatchInfo{},
		deltaChannelInfos: map[UniqueID][]*datapb.VchannelInfo{},
		segmentsInfo:      newSegmentsInfo(kv),
		replicas:          NewReplicaInfos(),
	}
	meta.setKvClient(kv)

	kvs := make(map[string]string)
	collectionInfo := &querypb.CollectionInfo{
		CollectionID: defaultCollectionID,
	}
	collectionBlobs, err := proto.Marshal(collectionInfo)
	assert.Nil(t, err)
	collectionKey := fmt.Sprintf("%s/%d", collectionMetaPrefix, defaultCollectionID)
	kvs[collectionKey] = string(collectionBlobs)

	deltaChannel1 := &datapb.VchannelInfo{
		CollectionID: defaultCollectionID,
		ChannelName:  "delta-channel1",
		FlushedSegments: []*datapb.SegmentInfo{{
			ID:           1,
			CollectionID: defaultCollectionID,
		}},
		UnflushedSegments: []*datapb.SegmentInfo{{
			ID:           2,
			CollectionID: defaultCollectionID,
		}},
		DroppedSegments: []*datapb.SegmentInfo{{
			ID:           3,
			CollectionID: defaultCollectionID,
		}},
	}
	deltaChannel2 := &datapb.VchannelInfo{
		CollectionID: defaultCollectionID,
		ChannelName:  "delta-channel2",
		FlushedSegments: []*datapb.SegmentInfo{{
			ID:           4,
			CollectionID: defaultCollectionID,
		}},
		UnflushedSegments: []*datapb.SegmentInfo{{
			ID:           5,
			CollectionID: defaultCollectionID,
		}},
		DroppedSegments: []*datapb.SegmentInfo{{
			ID:           6,
			CollectionID: defaultCollectionID,
		}},
	}

	infos := []*datapb.VchannelInfo{deltaChannel1, deltaChannel2}
	for _, info := range infos {
		infoBytes, err := proto.Marshal(info)
		assert.Nil(t, err)

		key := fmt.Sprintf("%s/%d/%s", deltaChannelMetaPrefix, defaultCollectionID, info.ChannelName)
		kvs[key] = string(infoBytes)
	}

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 1, len(meta.collectionInfos))
	collectionInfo, err = meta.getCollectionInfoByID(collectionInfo.CollectionID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(collectionInfo.ReplicaIds))
	assert.Equal(t, int32(1), collectionInfo.ReplicaNumber)

	channels, err := meta.getDeltaChannelsByCollectionID(collectionInfo.CollectionID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(channels[0].GetFlushedSegmentIds()))
	assert.Equal(t, 1, len(channels[0].GetUnflushedSegmentIds()))
	assert.Equal(t, 1, len(channels[0].GetDroppedSegmentIds()))
}

func TestSaveSegments(t *testing.T) {
	refreshParams()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	defer etcdCli.Close()
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)

	meta := &MetaReplica{
		collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
		dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
		segmentsInfo:    newSegmentsInfo(kv),
		replicas:        NewReplicaInfos(),
		client:          kv,
	}

	t.Run("LoadCollection", func(t *testing.T) {
		defer func() {
			meta.segmentsInfo = newSegmentsInfo(kv)
		}()
		eventChan := kv.WatchWithPrefix(util.ChangeInfoMetaPrefix)

		segmentNum := 5
		save := MockSaveSegments(segmentNum)
		log.Debug("save segments...",
			zap.Any("segments", save))
		meta.saveGlobalSealedSegInfos(save, nil)

		log.Debug("wait for etcd event")
		sawOnlineSegments := false
		for !sawOnlineSegments {
			watchResp, ok := <-eventChan
			assert.True(t, ok)

			for _, event := range watchResp.Events {
				changeInfoBatch := &querypb.SealedSegmentsChangeInfo{}
				err := proto.Unmarshal(event.Kv.Value, changeInfoBatch)
				assert.NoError(t, err)

				assert.Equal(t, segmentNum, len(changeInfoBatch.GetInfos()))
				for _, changeInfo := range changeInfoBatch.GetInfos() {
					assert.Empty(t, changeInfo.OfflineSegments)
					assert.Equal(t, 1, len(changeInfo.OnlineSegments))
				}

				sawOnlineSegments = true
			}
		}

	})

	t.Run("LoadBalance", func(t *testing.T) {
		defer func() {
			meta.segmentsInfo = newSegmentsInfo(kv)
		}()

		eventChan := kv.WatchWithPrefix(util.ChangeInfoMetaPrefix)

		segmentNum := 5
		save := MockSaveSegments(segmentNum)
		for _, segment := range save[defaultCollectionID] {
			meta.segmentsInfo.saveSegment(segment)
		}

		balancedSegment := &querypb.SegmentInfo{
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentID:    defaultSegmentID,
			DmChannel:    "testDmChannel",
			SegmentState: commonpb.SegmentState_Sealed,
			NodeIds:      []UniqueID{defaultQueryNodeID + 1},
		}

		save = map[int64][]*querypb.SegmentInfo{
			defaultCollectionID: {balancedSegment},
		}
		meta.saveGlobalSealedSegInfos(save, nil)

		sawOnlineSegments := false
		for !sawOnlineSegments {
			watchResp, ok := <-eventChan
			assert.True(t, ok)

			for _, event := range watchResp.Events {
				changeInfoBatch := &querypb.SealedSegmentsChangeInfo{}
				err := proto.Unmarshal(event.Kv.Value, changeInfoBatch)
				assert.NoError(t, err)

				assert.Equal(t, 1, len(changeInfoBatch.GetInfos()))
				for _, changeInfo := range changeInfoBatch.GetInfos() {
					assert.Equal(t, 1, len(changeInfo.OfflineSegments))
					assert.Equal(t, 1, len(changeInfo.OnlineSegments))

					assert.Equal(t, defaultQueryNodeID, changeInfo.OfflineSegments[0].NodeIds[0])
					assert.Equal(t, defaultQueryNodeID+1, changeInfo.OnlineSegments[0].NodeIds[0])
				}

				sawOnlineSegments = true
			}
		}
	})

	t.Run("Handoff", func(t *testing.T) {
		defer func() {
			meta.segmentsInfo = newSegmentsInfo(kv)
		}()

		eventChan := kv.WatchWithPrefix(util.ChangeInfoMetaPrefix)

		segmentNum := 5
		save := MockSaveSegments(segmentNum)
		for _, segment := range save[defaultCollectionID] {
			meta.segmentsInfo.saveSegment(segment)
		}

		spawnSegment := &querypb.SegmentInfo{
			CollectionID:   defaultCollectionID,
			PartitionID:    defaultPartitionID,
			SegmentID:      defaultSegmentID + int64(segmentNum),
			DmChannel:      "testDmChannel",
			SegmentState:   commonpb.SegmentState_Sealed,
			NodeIds:        []UniqueID{defaultQueryNodeID + 1},
			CompactionFrom: []UniqueID{defaultSegmentID, defaultSegmentID + 1},
		}

		save = map[int64][]*querypb.SegmentInfo{
			defaultCollectionID: {spawnSegment},
		}
		remove := map[int64][]*querypb.SegmentInfo{
			defaultCollectionID: {},
		}
		for _, segmentID := range spawnSegment.CompactionFrom {
			segment, err := meta.getSegmentInfoByID(segmentID)
			assert.NoError(t, err)

			remove[defaultCollectionID] = append(remove[defaultCollectionID],
				segment)
		}

		meta.saveGlobalSealedSegInfos(save, remove)

		sawOnlineSegment := false
		sawOfflineSegment := false
		for !sawOnlineSegment || !sawOfflineSegment {
			watchResp, ok := <-eventChan
			assert.True(t, ok)

			for _, event := range watchResp.Events {
				changeInfoBatch := &querypb.SealedSegmentsChangeInfo{}
				err := proto.Unmarshal(event.Kv.Value, changeInfoBatch)
				assert.NoError(t, err)

				for _, changeInfo := range changeInfoBatch.GetInfos() {
					if !sawOnlineSegment {
						assert.Equal(t, 1, len(changeInfo.OnlineSegments))
						assert.Equal(t, defaultSegmentID+int64(segmentNum), changeInfo.OnlineSegments[0].SegmentID)
						assert.Equal(t, defaultQueryNodeID+1, changeInfo.OnlineSegments[0].NodeIds[0])

						sawOnlineSegment = true
					} else {
						assert.Equal(t, len(spawnSegment.CompactionFrom), len(changeInfo.OfflineSegments))
						sawOfflineSegment = true
					}
				}
			}
		}
	})
}

func MockSaveSegments(segmentNum int) col2SegmentInfos {
	saves := make(col2SegmentInfos)
	segments := make([]*querypb.SegmentInfo, 0)
	for i := 0; i < segmentNum; i++ {
		segments = append(segments, &querypb.SegmentInfo{
			CollectionID: defaultCollectionID,
			PartitionID:  defaultPartitionID,
			SegmentID:    defaultSegmentID + int64(i),
			DmChannel:    "testDmChannel",
			SegmentState: commonpb.SegmentState_Sealed,
			NodeIds:      []UniqueID{defaultQueryNodeID},
		})
	}
	saves[defaultCollectionID] = segments

	return saves
}

type mockDataCoord struct {
	types.DataCoord
	mock.Mock
}

func (m *mockDataCoord) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*datapb.GetSegmentInfoResponse), args.Error(1)
}

type mockKV struct {
	kv.MetaKv
	mock.Mock
}

func (m *mockKV) Save(k, v string) error {
	args := m.Called(k, v)
	return args.Error(0)
}

func TestPatchSegmentInfo(t *testing.T) {

	t.Run("No flawed segments", func(t *testing.T) {
		mkv := &mockKV{}
		dc := &mockDataCoord{}

		meta := &MetaReplica{
			collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
			dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
			segmentsInfo:    newSegmentsInfo(mkv),
			replicas:        NewReplicaInfos(),
			client:          mkv,
		}

		mkv.Test(t)
		dc.Test(t)
		meta.segmentsInfo.segmentIDMap[1] = &querypb.SegmentInfo{SegmentID: 1, DmChannel: "dml_channel_01"}

		err := meta.patchSegmentInfo(context.Background(), dc)
		assert.NoError(t, err)

		mkv.Mock.AssertNotCalled(t, "Save", mock.AnythingOfType("string"), mock.AnythingOfType("string"))
		dc.Mock.AssertNotCalled(t, "GetSegmentInfo", mock.Anything, mock.Anything)
	})

	t.Run("with flawed segments", func(t *testing.T) {
		mkv := &mockKV{}
		dc := &mockDataCoord{}

		meta := &MetaReplica{
			collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
			dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
			segmentsInfo:    newSegmentsInfo(mkv),
			replicas:        NewReplicaInfos(),
			client:          mkv,
		}

		mkv.Test(t)
		dc.Test(t)
		meta.segmentsInfo.segmentIDMap[1] = &querypb.SegmentInfo{SegmentID: 1, DmChannel: ""}

		mkv.On("Save", mock.Anything, mock.Anything).Return(nil)
		dc.On("GetSegmentInfo", context.Background(), &datapb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs:       []UniqueID{1},
			IncludeUnHealthy: true,
		}).Return(&datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Infos: []*datapb.SegmentInfo{
				{ID: 1, InsertChannel: "dml_channel_01"},
			},
		}, nil)

		err := meta.patchSegmentInfo(context.Background(), dc)
		assert.NoError(t, err)

		info, err := meta.getSegmentInfoByID(1)
		assert.NoError(t, err)
		assert.NotEqual(t, "", info.DmChannel)
	})

	t.Run("GetSegmentInfo_error", func(t *testing.T) {
		mkv := &mockKV{}
		dc := &mockDataCoord{}

		meta := &MetaReplica{
			collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
			dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
			segmentsInfo:    newSegmentsInfo(mkv),
			replicas:        NewReplicaInfos(),
			client:          mkv,
			dataCoord:       dc,
		}

		mkv.Test(t)
		dc.Test(t)
		meta.segmentsInfo.segmentIDMap[1] = &querypb.SegmentInfo{SegmentID: 1, DmChannel: ""}

		mkv.On("Save", mock.Anything, mock.Anything).Return(nil)
		dc.On("GetSegmentInfo", context.Background(), &datapb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs:       []UniqueID{1},
			IncludeUnHealthy: true,
		}).Return((*datapb.GetSegmentInfoResponse)(nil), errors.New("mock"))

		err := meta.patchSegmentInfo(context.Background(), dc)
		assert.Error(t, err)
	})

	t.Run("GetSegmentInfo_fail", func(t *testing.T) {
		mkv := &mockKV{}
		dc := &mockDataCoord{}

		meta := &MetaReplica{
			collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
			dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
			segmentsInfo:    newSegmentsInfo(mkv),
			replicas:        NewReplicaInfos(),
			client:          mkv,
			dataCoord:       dc,
		}

		mkv.Test(t)
		dc.Test(t)
		meta.segmentsInfo.segmentIDMap[1] = &querypb.SegmentInfo{SegmentID: 1, DmChannel: ""}

		mkv.On("Save", mock.Anything, mock.Anything).Return(nil)
		dc.On("GetSegmentInfo", context.Background(), &datapb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs:       []UniqueID{1},
			IncludeUnHealthy: true,
		}).Return(&datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_SegmentNotFound},
		}, nil)

		err := meta.patchSegmentInfo(context.Background(), dc)
		assert.Error(t, err)
	})

	t.Run("segments patched not found", func(t *testing.T) {
		mkv := &mockKV{}
		dc := &mockDataCoord{}

		meta := &MetaReplica{
			collectionInfos: map[UniqueID]*querypb.CollectionInfo{},
			dmChannelInfos:  map[string]*querypb.DmChannelWatchInfo{},
			segmentsInfo:    newSegmentsInfo(mkv),
			replicas:        NewReplicaInfos(),
			client:          mkv,
		}

		mkv.Test(t)
		dc.Test(t)
		meta.segmentsInfo.segmentIDMap[1] = &querypb.SegmentInfo{SegmentID: 1, DmChannel: ""}

		mkv.On("Save", mock.Anything, mock.Anything).Return(nil)
		dc.On("GetSegmentInfo", context.Background(), &datapb.GetSegmentInfoRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SegmentInfo,
			},
			SegmentIDs:       []UniqueID{1},
			IncludeUnHealthy: true,
		}).Return(&datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Infos: []*datapb.SegmentInfo{
				{ID: 2, InsertChannel: "dml_channel_01"},
			},
		}, nil)

		err := meta.patchSegmentInfo(context.Background(), dc)
		assert.Error(t, err)
	})

}
