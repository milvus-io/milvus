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

package indexcoord

import (
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/stretchr/testify/assert"
)

func TestMetaTable(t *testing.T) {
	Params.Init()
	etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

	req := &indexpb.BuildIndexRequest{
		IndexBuildID: 1,
		IndexName:    "test_index",
		IndexID:      0,
		DataPaths:    []string{"DataPath-1-1", "DataPath-1-2"},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-2"}},
	}
	indexMeta1 := &indexpb.IndexMeta{
		IndexBuildID:   1,
		State:          commonpb.IndexState_Finished,
		Req:            req,
		IndexFilePaths: []string{"IndexFilePath-1-1", "IndexFilePath-1-2"},
		NodeID:         0,
		Version:        10,
		Recycled:       false,
	}
	value := proto.MarshalTextString(indexMeta1)
	key := "indexes/" + strconv.FormatInt(indexMeta1.IndexBuildID, 10)
	err = etcdKV.Save(key, value)
	assert.Nil(t, err)
	metaTable, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	assert.NotNil(t, metaTable)

	t.Run("saveIndexMeta", func(t *testing.T) {
		meta := &Meta{
			indexMeta: indexMeta1,
			revision:  10,
		}
		err = metaTable.saveIndexMeta(meta)
		assert.NotNil(t, err)
	})

	t.Run("reloadMeta", func(t *testing.T) {
		indexBuildID := UniqueID(3)
		meta3, err := metaTable.reloadMeta(indexBuildID)
		assert.NotNil(t, err)
		assert.Nil(t, meta3)
	})

	t.Run("AddIndex", func(t *testing.T) {
		req := &indexpb.BuildIndexRequest{
			IndexBuildID: 1,
		}
		err = metaTable.AddIndex(req.IndexBuildID, req)
		assert.NotNil(t, err)
	})

	t.Run("GetIndexMetaByIndexBuildID", func(t *testing.T) {
		indexMeta := metaTable.GetIndexMetaByIndexBuildID(1)
		assert.NotNil(t, indexMeta)

		indexMeta2 := metaTable.GetIndexMetaByIndexBuildID(20)
		assert.Nil(t, indexMeta2)
	})

	t.Run("BuildIndex", func(t *testing.T) {
		err = metaTable.BuildIndex(UniqueID(4), 1)
		assert.NotNil(t, err)

		indexMeta1.NodeID = 2
		value = proto.MarshalTextString(indexMeta1)
		key = "indexes/" + strconv.FormatInt(indexMeta1.IndexBuildID, 10)
		err = etcdKV.Save(key, value)
		assert.Nil(t, err)
		err = metaTable.BuildIndex(indexMeta1.IndexBuildID, 1)
		assert.Nil(t, err)
	})

	t.Run("UpdateVersion", func(t *testing.T) {
		err = metaTable.UpdateVersion(UniqueID(5))
		assert.NotNil(t, err)

		indexMeta1.Version = indexMeta1.Version + 1
		value = proto.MarshalTextString(indexMeta1)
		key = "indexes/" + strconv.FormatInt(indexMeta1.IndexBuildID, 10)
		err = etcdKV.Save(key, value)
		assert.Nil(t, err)
		err = metaTable.UpdateVersion(indexMeta1.IndexBuildID)
		assert.Nil(t, err)
	})

	t.Run("MarkIndexAsDeleted", func(t *testing.T) {
		indexMeta1.Version = indexMeta1.Version + 1
		value = proto.MarshalTextString(indexMeta1)
		key = "indexes/" + strconv.FormatInt(indexMeta1.IndexBuildID, 10)
		err = etcdKV.Save(key, value)
		assert.Nil(t, err)
		err = metaTable.MarkIndexAsDeleted(indexMeta1.Req.IndexID)
		assert.Nil(t, err)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		indexInfos := metaTable.GetIndexStates([]UniqueID{0, 1})
		assert.Equal(t, 2, len(indexInfos))
		assert.Equal(t, "index 0 not exists", indexInfos[0].Reason)
		assert.Equal(t, "index 1 has been deleted", indexInfos[1].Reason)
	})

	t.Run("GetIndexFilePathInfo", func(t *testing.T) {
		indexFilePathInfo, err := metaTable.GetIndexFilePathInfo(0)
		assert.Nil(t, indexFilePathInfo)
		assert.NotNil(t, err)

		indexFilePathInfo2, err := metaTable.GetIndexFilePathInfo(1)
		assert.Nil(t, indexFilePathInfo2)
		assert.NotNil(t, err)
	})

	t.Run("UpdateRecycleState", func(t *testing.T) {
		indexMeta1.Version = indexMeta1.Version + 1
		value = proto.MarshalTextString(indexMeta1)
		key = "indexes/" + strconv.FormatInt(indexMeta1.IndexBuildID, 10)
		err = etcdKV.Save(key, value)
		assert.Nil(t, err)

		err = metaTable.UpdateRecycleState(indexMeta1.IndexBuildID)
		assert.Nil(t, err)

		err = metaTable.UpdateRecycleState(indexMeta1.IndexBuildID)
		assert.Nil(t, err)

		err = metaTable.UpdateRecycleState(5)
		assert.NotNil(t, err)
	})

	t.Run("HasSameReq", func(t *testing.T) {
		req2 := &indexpb.BuildIndexRequest{
			IndexBuildID: 6,
			IndexName:    "test_index",
			IndexID:      2,
			DataPaths:    []string{"DataPath-1-1", "DataPath-1-2"},
			TypeParams:   []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}},
			IndexParams:  []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-2"}},
		}

		err = metaTable.AddIndex(6, req2)
		assert.Nil(t, err)

		req3 := &indexpb.BuildIndexRequest{
			IndexBuildID: 6,
			IndexName:    "test_index",
			IndexID:      3,
			DataPaths:    []string{"DataPath-1-1", "DataPath-1-2"},
			TypeParams:   []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}},
			IndexParams:  []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-2"}},
		}

		has, err := metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.IndexID = 2
		req3.IndexName = "test_index1"
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.IndexName = "test_index"
		req3.DataPaths = []string{"DataPath-1-1", "DataPath-1-2", "DataPath-1-3"}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.DataPaths = []string{"DataPath-1-1", "DataPath-1-3"}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.DataPaths = []string{"DataPath-1-1", "DataPath-1-2"}
		req3.TypeParams = []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.TypeParams = []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-3", Value: "TypeParam-1-3"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.TypeParams = []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-3"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.TypeParams = []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}}
		req3.IndexParams = []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.IndexParams = []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-3", Value: "IndexParam-1-3"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)

		req3.IndexParams = []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-3"}}
		has, err = metaTable.HasSameReq(req3)
		assert.Equal(t, false, has)
		assert.NotNil(t, err)
	})

	t.Run("LoadMetaFromETCD", func(t *testing.T) {
		req4 := &indexpb.BuildIndexRequest{
			IndexBuildID: 7,
			IndexName:    "test_index",
			IndexID:      4,
			DataPaths:    []string{"DataPath-1-1", "DataPath-1-2"},
			TypeParams:   []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}},
			IndexParams:  []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-2"}},
		}
		err = metaTable.AddIndex(7, req4)
		assert.Nil(t, err)

		ok := metaTable.LoadMetaFromETCD(8, 0)
		assert.Equal(t, false, ok)

		key = "indexes/" + strconv.FormatInt(req4.IndexBuildID, 10)
		err = etcdKV.RemoveWithPrefix(key)
		assert.Nil(t, err)

		ok = metaTable.LoadMetaFromETCD(req4.IndexBuildID, 10)
		assert.Equal(t, false, ok)
	})

	t.Run("GetNodeTaskStats", func(t *testing.T) {
		req5 := &indexpb.BuildIndexRequest{
			IndexBuildID: 9,
			IndexName:    "test_index",
			IndexID:      5,
			DataPaths:    []string{"DataPath-1-1", "DataPath-1-2"},
			TypeParams:   []*commonpb.KeyValuePair{{Key: "TypeParam-1-1", Value: "TypeParam-1-1"}, {Key: "TypeParam-1-2", Value: "TypeParam-1-2"}},
			IndexParams:  []*commonpb.KeyValuePair{{Key: "IndexParam-1-1", Value: "IndexParam-1-1"}, {Key: "IndexParam-1-2", Value: "IndexParam-1-2"}},
		}

		err = metaTable.AddIndex(req5.IndexBuildID, req5)
		assert.Nil(t, err)

		err = metaTable.BuildIndex(req5.IndexBuildID, 4)
		assert.Nil(t, err)

		priorities := metaTable.GetNodeTaskStats()
		assert.Equal(t, 1, priorities[4])
	})

	err = etcdKV.RemoveWithPrefix("indexes/")
	assert.Nil(t, err)
}

func TestMetaTable_Error(t *testing.T) {
	Params.Init()
	etcdKV, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

	t.Run("reloadFromKV error", func(t *testing.T) {
		value := "indexMeta-1"
		key := "indexes/" + strconv.FormatInt(2, 10)
		err = etcdKV.Save(key, value)
		assert.Nil(t, err)
		meta, err := NewMetaTable(etcdKV)
		assert.NotNil(t, err)
		assert.Nil(t, meta)
		err = etcdKV.RemoveWithPrefix(key)
		assert.Nil(t, err)
	})

	err = etcdKV.RemoveWithPrefix("indexes/")
	assert.Nil(t, err)
}
