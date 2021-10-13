// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querycoord

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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

func (tk *testKv) Remove(key string) error {
	return tk.returnFn()
}

func (tk *testKv) LoadWithPrefix(key string) ([]string, []string, error) {
	return nil, nil, nil
}

func TestReplica_Release(t *testing.T) {
	refreshParams()
	etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	meta, err := newMeta(etcdKV)
	assert.Nil(t, err)
	err = meta.addCollection(1, nil)
	require.NoError(t, err)

	collections := meta.showCollections()
	assert.Equal(t, 1, len(collections))

	err = meta.addPartition(1, 100)
	assert.NoError(t, err)
	partitions, err := meta.showPartitions(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(partitions))

	meta.releasePartition(1, 100)
	partitions, err = meta.showPartitions(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(partitions))
	meta.releasePartition(1, 100)

	meta.releaseCollection(1)
	collections = meta.showCollections()
	assert.Equal(t, 0, len(collections))
	meta.releaseCollection(1)
}

func TestMetaFunc(t *testing.T) {
	refreshParams()
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	meta := &MetaReplica{
		client:            kv,
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		segmentInfos:      map[UniqueID]*querypb.SegmentInfo{},
		queryChannelInfos: map[UniqueID]*querypb.QueryChannelInfo{},
	}

	nodeID := int64(100)
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

	t.Run("Test HasSegmentInfoFalse", func(t *testing.T) {
		hasSegmentInfo := meta.hasSegmentInfo(defaultSegmentID)
		assert.Equal(t, false, hasSegmentInfo)
	})

	t.Run("Test GetSegmentInfoByIDFail", func(t *testing.T) {
		res, err := meta.getSegmentInfoByID(defaultSegmentID)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test GetCollectionInfoByIDFail", func(t *testing.T) {
		res, err := meta.getCollectionInfoByID(defaultCollectionID)
		assert.Nil(t, res)
		assert.NotNil(t, err)
	})

	t.Run("Test GetQueryChannelInfoByIDFail", func(t *testing.T) {
		res, err := meta.getQueryChannelInfoByID(defaultCollectionID)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test GetPartitionStatesByIDFail", func(t *testing.T) {
		res, err := meta.getPartitionStatesByID(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, res)
		assert.NotNil(t, err)
	})

	t.Run("Test GetDmChannelsByNodeIDFail", func(t *testing.T) {
		res, err := meta.getDmChannelsByNodeID(defaultCollectionID, nodeID)
		assert.NotNil(t, err)
		assert.Nil(t, res)
	})

	t.Run("Test AddDmChannelFail", func(t *testing.T) {
		err := meta.addDmChannel(defaultCollectionID, nodeID, dmChannels)
		assert.NotNil(t, err)
	})

	t.Run("Test SetLoadTypeFail", func(t *testing.T) {
		err := meta.setLoadType(defaultCollectionID, querypb.LoadType_loadCollection)
		assert.NotNil(t, err)
	})

	t.Run("Test SetLoadPercentageFail", func(t *testing.T) {
		err := meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_loadCollection)
		assert.NotNil(t, err)
	})

	t.Run("Test AddCollection", func(t *testing.T) {
		schema := genCollectionSchema(defaultCollectionID, false)
		err := meta.addCollection(defaultCollectionID, schema)
		assert.Nil(t, err)
	})

	t.Run("Test HasCollection", func(t *testing.T) {
		hasCollection := meta.hasCollection(defaultCollectionID)
		assert.Equal(t, true, hasCollection)
	})

	t.Run("Test AddPartition", func(t *testing.T) {
		err := meta.addPartition(defaultCollectionID, defaultPartitionID)
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
		err := meta.addDmChannel(defaultCollectionID, nodeID, dmChannels)
		assert.Nil(t, err)
	})

	t.Run("Test GetDmChannelsByNodeID", func(t *testing.T) {
		channels, err := meta.getDmChannelsByNodeID(defaultCollectionID, nodeID)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(channels))
	})

	t.Run("Test SetSegmentInfo", func(t *testing.T) {
		info := &querypb.SegmentInfo{
			SegmentID:    defaultSegmentID,
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
			NodeID:       nodeID,
		}
		err := meta.setSegmentInfo(defaultSegmentID, info)
		assert.Nil(t, err)
	})

	t.Run("Test ShowSegmentInfo", func(t *testing.T) {
		infos := meta.showSegmentInfos(defaultCollectionID, []UniqueID{defaultPartitionID})
		assert.Equal(t, 1, len(infos))
		assert.Equal(t, defaultSegmentID, infos[0].SegmentID)
	})

	t.Run("Test getQueryChannel", func(t *testing.T) {
		reqChannel, resChannel, err := meta.getQueryChannel(defaultCollectionID)
		assert.NotNil(t, reqChannel)
		assert.NotNil(t, resChannel)
		assert.Nil(t, err)
	})

	t.Run("Test GetSegmentInfoByID", func(t *testing.T) {
		info, err := meta.getSegmentInfoByID(defaultSegmentID)
		assert.Nil(t, err)
		assert.Equal(t, defaultSegmentID, info.SegmentID)
	})

	t.Run("Test SetLoadType", func(t *testing.T) {
		err := meta.setLoadType(defaultCollectionID, querypb.LoadType_loadCollection)
		assert.Nil(t, err)
	})

	t.Run("Test SetLoadPercentage", func(t *testing.T) {
		err := meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_LoadPartition)
		assert.Nil(t, err)
		state, err := meta.getPartitionStatesByID(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, err)
		assert.Equal(t, int64(100), state.InMemoryPercentage)
		err = meta.setLoadPercentage(defaultCollectionID, defaultPartitionID, 100, querypb.LoadType_loadCollection)
		assert.Nil(t, err)
		info, err := meta.getCollectionInfoByID(defaultCollectionID)
		assert.Nil(t, err)
		assert.Equal(t, int64(100), info.InMemoryPercentage)
	})

	t.Run("Test RemoveDmChannel", func(t *testing.T) {
		err := meta.removeDmChannel(defaultCollectionID, nodeID, dmChannels)
		assert.Nil(t, err)
		channels, err := meta.getDmChannelsByNodeID(defaultCollectionID, nodeID)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(channels))
	})

	t.Run("Test DeleteSegmentInfoByNodeID", func(t *testing.T) {
		err := meta.deleteSegmentInfoByNodeID(nodeID)
		assert.Nil(t, err)
		_, err = meta.getSegmentInfoByID(defaultSegmentID)
		assert.NotNil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		err := meta.releasePartition(defaultCollectionID, defaultPartitionID)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		err := meta.releaseCollection(defaultCollectionID)
		assert.Nil(t, err)
	})
}

func TestReloadMetaFromKV(t *testing.T) {
	refreshParams()
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	meta := &MetaReplica{
		client:            kv,
		collectionInfos:   map[UniqueID]*querypb.CollectionInfo{},
		segmentInfos:      map[UniqueID]*querypb.SegmentInfo{},
		queryChannelInfos: map[UniqueID]*querypb.QueryChannelInfo{},
	}

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
	segmentKey := fmt.Sprintf("%s/%d", segmentMetaPrefix, defaultSegmentID)
	kvs[segmentKey] = string(segmentBlobs)

	queryChannelInfo := &querypb.QueryChannelInfo{
		CollectionID: defaultCollectionID,
	}
	queryChannelBlobs, err := proto.Marshal(queryChannelInfo)
	assert.Nil(t, err)
	queryChannelKey := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, defaultCollectionID)
	kvs[queryChannelKey] = string(queryChannelBlobs)

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

	assert.Equal(t, 1, len(meta.collectionInfos))
	assert.Equal(t, 1, len(meta.segmentInfos))
	assert.Equal(t, 1, len(meta.queryChannelInfos))
	_, ok := meta.collectionInfos[defaultCollectionID]
	assert.Equal(t, true, ok)
	_, ok = meta.segmentInfos[defaultSegmentID]
	assert.Equal(t, true, ok)
	_, ok = meta.queryChannelInfos[defaultCollectionID]
	assert.Equal(t, true, ok)
}
