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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	meta, err := newMeta(context.Background(), etcdKV, nil, idAllocator, nil)
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
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		queryChannelInfos: map[UniqueID]*querypb.QueryChannelInfo{},
		dmChannelInfos:    map[string]*querypb.DmChannelWatchInfo{},
		segmentsInfo:      segmentsInfo,
		replicas:          NewReplicaInfos(),
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

	t.Run("Test GetQueryChannelInfoByIDFirst", func(t *testing.T) {
		res := meta.getQueryChannelInfoByID(defaultCollectionID)
		assert.NotNil(t, res)
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

	t.Run("Test getQueryChannelSecond", func(t *testing.T) {
		info := meta.getQueryChannelInfoByID(defaultCollectionID)
		assert.NotNil(t, info.QueryChannel)
		assert.NotNil(t, info.QueryResultChannel)
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
		queryChannelInfos: map[UniqueID]*querypb.QueryChannelInfo{},
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
		SegmentID: defaultSegmentID,
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

func TestCreateQueryChannel(t *testing.T) {
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

	fixedQueryChannel := Params.CommonCfg.QueryCoordSearch + "-0"
	fixedQueryResultChannel := Params.CommonCfg.QueryCoordSearchResult + "-0"

	tests := []struct {
		inID             UniqueID
		outQueryChannel  string
		outResultChannel string

		description string
	}{
		{0, fixedQueryChannel, fixedQueryResultChannel, "collection ID = 0"},
		{1, fixedQueryChannel, fixedQueryResultChannel, "collection ID = 1"},
	}

	m := &MetaReplica{
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		queryChannelInfos: map[UniqueID]*querypb.QueryChannelInfo{},
		dmChannelInfos:    map[string]*querypb.DmChannelWatchInfo{},
		segmentsInfo:      segmentsInfo,
	}
	m.setKvClient(kv)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			info := m.createQueryChannel(test.inID)
			assert.Equal(t, info.GetQueryChannel(), test.outQueryChannel)
			assert.Equal(t, info.GetQueryResultChannel(), test.outResultChannel)
		})
	}
}

func TestGetDataSegmentInfosByIDs(t *testing.T) {
	dataCoord := &dataCoordMock{}
	meta := &MetaReplica{
		dataCoord: dataCoord,
	}

	segmentInfos, err := meta.getDataSegmentInfosByIDs([]int64{1})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(segmentInfos))

	dataCoord.returnError = true
	segmentInfos2, err := meta.getDataSegmentInfosByIDs([]int64{1})
	assert.Error(t, err)
	assert.Empty(t, segmentInfos2)

	dataCoord.returnError = false
	dataCoord.returnGrpcError = true
	segmentInfos3, err := meta.getDataSegmentInfosByIDs([]int64{1})
	assert.Error(t, err)
	assert.Empty(t, segmentInfos3)
}
