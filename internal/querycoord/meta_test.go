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
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

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
	collectionBlobs := proto.MarshalTextString(collectionInfo)
	collectionKey := fmt.Sprintf("%s/%d", collectionMetaPrefix, defaultCollectionID)
	kvs[collectionKey] = collectionBlobs

	segmentInfo := &querypb.SegmentInfo{
		SegmentID: defaultSegmentID,
	}
	segmentBlobs := proto.MarshalTextString(segmentInfo)
	segmentKey := fmt.Sprintf("%s/%d", segmentMetaPrefix, defaultSegmentID)
	kvs[segmentKey] = segmentBlobs

	queryChannelInfo := &querypb.QueryChannelInfo{
		CollectionID: defaultCollectionID,
	}
	queryChannelBlobs := proto.MarshalTextString(queryChannelInfo)
	queryChannelKey := fmt.Sprintf("%s/%d", queryChannelMetaPrefix, defaultCollectionID)
	kvs[queryChannelKey] = queryChannelBlobs

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
