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

package indexcoord

import (
	"errors"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/stretchr/testify/assert"
)

var (
	collID    = UniqueID(100)
	partID    = UniqueID(200)
	segID     = UniqueID(300)
	indexID   = UniqueID(400)
	buildID   = UniqueID(500)
	fieldID   = UniqueID(600)
	nodeID    = UniqueID(700)
	invalidID = UniqueID(999)

	indexName = "index"

	createTs = uint64(10)
)

func TestNewMetaTable(t *testing.T) {
	segIdx := &indexpb.SegmentIndex{
		CollectionID: collID,
		PartitionID:  partID,
		SegmentID:    segID,
		NumRows:      1024,
		IndexID:      indexID,
		BuildID:      buildID,
		NodeID:       0,
		IndexVersion: 1,
		State:        commonpb.IndexState_InProgress,
		CreateTime:   createTs,
	}
	value1, err := proto.Marshal(segIdx)
	assert.NoError(t, err)

	index := &indexpb.FieldIndex{
		IndexInfo: &indexpb.IndexInfo{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
		},
	}
	value2, err := proto.Marshal(index)
	assert.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				if key == util.SegmentIndexPrefix {
					return []string{"1"}, []string{string(value1)}, nil
				}
				return []string{"1"}, []string{string(value2)}, nil
			},
		}
		mt, err := NewMetaTable(kv)
		assert.NoError(t, err)
		assert.NotNil(t, mt)
	})

	t.Run("load collection index error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{}, []string{}, errors.New("error occurred")
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})

	t.Run("load segment index error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithPrefix: func(s string) ([]string, []string, error) {
				if s == util.FieldIndexPrefix {
					return []string{"1"}, []string{string(value2)}, nil
				}
				return nil, nil, errors.New("error")
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})
}

func constructMetaTable(catalog metastore.IndexCoordCatalog) *metaTable {
	return &metaTable{
		catalog:          catalog,
		indexLock:        sync.RWMutex{},
		segmentIndexLock: sync.RWMutex{},
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: &model.Index{
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   createTs,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   "dim",
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   "nprobe",
							Value: "16",
						},
					},
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: &model.SegmentIndex{
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1024,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        0,
					IndexState:    commonpb.IndexState_Finished,
					FailReason:    "",
					IndexVersion:  1,
					IsDeleted:     false,
					CreateTime:    createTs,
					IndexFileKeys: []string{"file1", "file2"},
					IndexSize:     1024,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1024,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        0,
				IndexState:    commonpb.IndexState_Finished,
				FailReason:    "",
				IndexVersion:  1,
				IsDeleted:     false,
				CreateTime:    createTs,
				IndexFileKeys: []string{"file1", "file2"},
				IndexSize:     1024,
			},
		},
	}
}

func TestMetaTable_GetAllIndexMeta(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	segIndexes := mt.GetAllIndexMeta()
	assert.Equal(t, 1, len(segIndexes))
}

func TestMetaTable_GetMeta(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	segIdx, exist := mt.GetMeta(buildID)
	assert.NotNil(t, segIdx)
	assert.True(t, exist)

	segIdx2, exist2 := mt.GetMeta(invalidID)
	assert.Nil(t, segIdx2)
	assert.False(t, exist2)
}

func TestMetaTable_GetTypeParams(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	typeParams1 := mt.GetTypeParams(collID, indexID)
	assert.Equal(t, 1, len(typeParams1))

	typeParams2 := mt.GetTypeParams(invalidID, indexID)
	assert.Nil(t, typeParams2)

	typeParams3 := mt.GetTypeParams(collID, invalidID)
	assert.Nil(t, typeParams3)
}

func TestMetaTable_GetIndexParams(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	indexParams1 := mt.GetIndexParams(collID, indexID)
	assert.Equal(t, 1, len(indexParams1))

	indexParams2 := mt.GetIndexParams(invalidID, indexID)
	assert.Nil(t, indexParams2)

	indexParams3 := mt.GetIndexParams(collID, invalidID)
	assert.Nil(t, indexParams3)
}

func TestMetaTable_CreateIndex(t *testing.T) {
	newIndexID := indexID + 2

	index := &model.Index{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexID:      newIndexID,
		IndexName:    "index_name3",
		TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
		IndexParams:  []*commonpb.KeyValuePair{{Key: "nprobe", Value: "32"}},
		IsDeleted:    false,
		CreateTime:   0,
	}
	t.Run("success", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
		err := mt.CreateIndex(index)
		assert.NoError(t, err)
	})
	t.Run("save failed", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return errors.New("error")
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
		err := mt.CreateIndex(index)
		assert.Error(t, err)
	})
}

func TestMetaTable_AddIndex(t *testing.T) {
	newBuildID := buildID + 2
	segIdx := &model.SegmentIndex{
		SegmentID:     segID,
		CollectionID:  collID,
		PartitionID:   partID,
		NumRows:       1024,
		IndexID:       indexID,
		BuildID:       newBuildID,
		NodeID:        0,
		IndexState:    commonpb.IndexState_IndexStateNone,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("success", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		_, ok := mt.buildID2SegmentIndex[newBuildID]
		assert.True(t, ok)

		err = mt.AddIndex(segIdx)
		assert.NoError(t, err)
	})

	t.Run("save failed", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return errors.New("error")
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
		err := mt.AddIndex(segIdx)
		assert.Error(t, err)
	})
}

func TestMetaTable_UpdateVersion(t *testing.T) {
	newBuildID := buildID + 3
	segIdx := &model.SegmentIndex{
		SegmentID:     segID,
		IndexID:       indexID,
		BuildID:       newBuildID,
		NodeID:        0,
		IndexState:    commonpb.IndexState_IndexStateNone,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}
	t.Run("success", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return nil
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return nil
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})

		err := mt.UpdateVersion(newBuildID+1, nodeID)
		assert.Error(t, err)

		err = mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.Error(t, err)

		err = mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID
		})
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.Error(t, err)

		err = mt.UpdateVersion(buildID, nodeID)
		assert.Error(t, err)
	})

	t.Run("save etcd fail", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return errors.New("error")
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})

		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.Error(t, err)
	})
}

func TestMetaTable_BuildIndex(t *testing.T) {
	newBuildID := buildID + 4
	segIdx := &model.SegmentIndex{
		SegmentID:     segID,
		IndexID:       indexID,
		BuildID:       newBuildID,
		NodeID:        0,
		IndexState:    commonpb.IndexState_IndexStateNone,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}
	t.Run("success and fail", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return nil
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})

		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.NoError(t, err)

		err = mt.BuildIndex(newBuildID)
		assert.NoError(t, err)

		err = mt.BuildIndex(newBuildID + 1)
		assert.Error(t, err)

		err = mt.BuildIndex(buildID)
		assert.Error(t, err)

		err = mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID
		})
		assert.NoError(t, err)

		err = mt.BuildIndex(newBuildID)
		assert.Error(t, err)
	})

	t.Run("etcd save fail", func(t *testing.T) {
		kv := &mockETCDKV{
			save: func(s string, s2 string) error {
				return errors.New("error")
			},
		}
		mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})

		err := mt.AddIndex(segIdx)
		assert.Error(t, err)

		err = mt.UpdateVersion(newBuildID, nodeID)
		assert.Error(t, err)

		err = mt.BuildIndex(newBuildID)
		assert.Error(t, err)
	})
}

func TestMetaTable_GetIndexesForCollection(t *testing.T) {
	kv := &mockETCDKV{
		save: func(s string, s2 string) error {
			return nil
		},
		multiSave: func(m map[string]string) error {
			return nil
		},
	}
	mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
	indexes := mt.GetIndexesForCollection(collID, "")
	assert.Equal(t, 1, len(indexes))

	err := mt.MarkIndexAsDeleted(collID, []UniqueID{indexID})
	assert.NoError(t, err)

	indexes2 := mt.GetIndexesForCollection(collID, "")
	assert.Equal(t, 0, len(indexes2))
}

func TestMetaTable_HasSameReq(t *testing.T) {
	req := &indexpb.CreateIndexRequest{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexName:    indexName,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: "128",
			},
		},
		IndexParams: []*commonpb.KeyValuePair{
			{
				Key:   "nprobe",
				Value: "16",
			},
		},
	}
	kv := &mockETCDKV{
		save: func(s string, s2 string) error {
			return nil
		},
		multiSave: func(m map[string]string) error {
			return nil
		},
	}

	mt := constructMetaTable(&indexcoord.Catalog{Txn: kv})
	exist, existIndexID := mt.HasSameReq(req)
	assert.True(t, exist)
	assert.Equal(t, indexID, existIndexID)

	req.FieldID = fieldID + 1
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	req.FieldID = fieldID
	req.IndexName = "indexName2"
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	req.IndexName = indexName
	req.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
		{
			Key:   "type",
			Value: "float",
		},
	}
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	req.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "256",
		},
	}
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	req.TypeParams = []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
	}
	req.IndexParams = []*commonpb.KeyValuePair{
		{
			Key:   "nprobe",
			Value: "16",
		},
		{
			Key:   "type",
			Value: "FLAT",
		},
	}
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	req.IndexParams = []*commonpb.KeyValuePair{
		{
			Key:   "nprobe",
			Value: "32",
		},
	}
	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)

	err := mt.MarkIndexAsDeleted(collID, []UniqueID{indexID})
	assert.Nil(t, err)

	exist, existIndexID = mt.HasSameReq(req)
	assert.False(t, exist)
	assert.Zero(t, existIndexID)
}

func TestMetaTable_CheckBuiltIndex(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	exist, buildID2 := mt.HasSameIndex(segID, indexID)
	assert.True(t, exist)
	assert.Equal(t, buildID, buildID2)

	exist, buildID2 = mt.HasSameIndex(segID+1, indexID)
	assert.False(t, exist)
	assert.Zero(t, buildID2)

	exist, buildID2 = mt.HasSameIndex(segID, indexID+1)
	assert.False(t, exist)
	assert.Zero(t, buildID2)
}

func TestMetaTable_GetIndexIDByName(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	indexID2CreateTs := mt.GetIndexIDByName(collID, indexName)
	assert.Equal(t, 1, len(indexID2CreateTs))

	indexID2CreateTs = mt.GetIndexIDByName(collID, "name2")
	assert.Equal(t, 0, len(indexID2CreateTs))

	indexID2CreateTs = mt.GetIndexIDByName(collID+1, "name3")
	assert.Equal(t, 0, len(indexID2CreateTs))
}

func TestMetaTable_GetIndexNameByID(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	name := mt.GetIndexNameByID(collID, indexID)
	assert.Equal(t, indexName, name)

	name = mt.GetIndexNameByID(collID, indexID+1)
	assert.Equal(t, "", name)
}

func TestMetaTable_GetIndexStates(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	states, stateCnt := mt.GetIndexStates(indexID, 11)
	assert.Equal(t, 1, len(states))
	assert.Equal(t, 1, stateCnt.Finished)
}

func TestMetaTable_GetIndexStates_Filter(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	states, stateCnt := mt.GetIndexStates(indexID, 11, func(_ *model.SegmentIndex) bool { return false })
	assert.Equal(t, 0, len(states))
	assert.Equal(t, 0, stateCnt.Finished)
}

func TestMetaTable_GetSegmentIndexes(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	segIdxes := mt.GetSegmentIndexes(segID)
	assert.Equal(t, 1, len(segIdxes))
}

func TestMetaTable_GetSegmentIndexState(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	state := mt.GetSegmentIndexState(segID)
	assert.Equal(t, commonpb.IndexState_Finished, state.state)

	state = mt.GetSegmentIndexState(segID + 1)
	assert.Equal(t, commonpb.IndexState_Finished, state.state)
}

func TestMetaTable_GetIndexBuildProgress(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: NewMockEtcdKV(),
	})
	indexRows := mt.GetIndexBuildProgress(indexID, []UniqueID{segID})
	assert.Equal(t, int64(1024), indexRows)

	indexRows = mt.GetIndexBuildProgress(indexID+1, []UniqueID{segID})
	assert.Equal(t, int64(0), indexRows)

	indexRows = mt.GetIndexBuildProgress(indexID, []UniqueID{segID + 1})
	assert.Equal(t, int64(0), indexRows)

	err := mt.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID})
	assert.NoError(t, err)

	indexRows = mt.GetIndexBuildProgress(indexID, []UniqueID{segID})
	assert.Equal(t, int64(0), indexRows)
}

func TestMetaTable_MarkIndexAsDeleted(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})

		err := mt.MarkIndexAsDeleted(collID+1, []UniqueID{indexID})
		assert.NoError(t, err)

		err = mt.MarkIndexAsDeleted(collID, []UniqueID{indexID, indexID + 1})
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})

		err := mt.MarkIndexAsDeleted(collID, []UniqueID{indexID})
		assert.Error(t, err)
	})
}

func TestMetaTable_MarkSegmentsIndexAsDeleted(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})

		err := mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID
		})
		assert.NoError(t, err)

		err = mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID
		})
		assert.NoError(t, err)

		err = mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID+1
		})
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})

		err := mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
			return segIndex.SegmentID == segID
		})
		assert.Error(t, err)
	})
}

func TestMetaTable_GetSegmentIndexByBuildID(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
		}})
	canRecycle, segIdx := mt.GetSegmentIndexByBuildID(buildID)
	assert.True(t, canRecycle)
	assert.ElementsMatch(t, []string{"file1", "file2"}, segIdx.IndexFileKeys)

	segIdx = &model.SegmentIndex{
		SegmentID:     segID + 1,
		CollectionID:  collID,
		PartitionID:   partID,
		NumRows:       1026,
		IndexID:       indexID,
		BuildID:       buildID + 1,
		NodeID:        0,
		IndexVersion:  0,
		IndexState:    commonpb.IndexState_Unissued,
		FailReason:    "",
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}
	err := mt.AddIndex(segIdx)
	assert.NoError(t, err)

	canRecycle, segIdx = mt.GetSegmentIndexByBuildID(buildID + 1)
	assert.False(t, canRecycle)
	assert.Nil(t, segIdx)

	canRecycle, segIdx = mt.GetSegmentIndexByBuildID(buildID + 2)
	assert.False(t, canRecycle)
	assert.Nil(t, segIdx)
}

func TestMetaTable_IsIndexDeleted(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	})

	deleted := mt.IsIndexDeleted(collID, indexID)
	assert.False(t, deleted)

	err := mt.MarkIndexAsDeleted(collID, []int64{indexID})
	assert.NoError(t, err)

	err = mt.MarkIndexAsDeleted(collID, []int64{indexID})
	assert.NoError(t, err)

	deleted = mt.IsIndexDeleted(collID, indexID)
	assert.True(t, deleted)

	deleted = mt.IsIndexDeleted(collID+1, indexID)
	assert.True(t, deleted)

	deleted = mt.IsIndexDeleted(collID, indexID+1)
	assert.True(t, deleted)
}

func TestMetaTable_IsSegIndexDeleted(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	})

	deleted := mt.IsSegIndexDeleted(buildID)
	assert.False(t, deleted)

	err := mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
		return segIndex.SegmentID == segID
	})
	assert.NoError(t, err)
	deleted = mt.IsSegIndexDeleted(buildID)
	assert.True(t, deleted)

	deleted = mt.IsSegIndexDeleted(buildID + 1)
	assert.True(t, deleted)
}

func TestMetaTable_GetMetasByNodeID(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			save: func(s string, s2 string) error {
				return nil
			},
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	})
	err := mt.AddIndex(&model.SegmentIndex{
		SegmentID:    segID + 1,
		CollectionID: collID,
		PartitionID:  partID,
		NumRows:      1025,
		IndexID:      indexID,
		BuildID:      buildID + 1,
		NodeID:       0,
	})
	assert.NoError(t, err)
	err = mt.UpdateVersion(buildID+1, nodeID)
	assert.NoError(t, err)
	segIdxes := mt.GetMetasByNodeID(nodeID)
	assert.Equal(t, 1, len(segIdxes))

	err = mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
		return segIndex.SegmentID == segID+1
	})
	assert.NoError(t, err)

	segIdxes = mt.GetMetasByNodeID(nodeID)
	assert.Equal(t, 0, len(segIdxes))
}

func TestMetaTable_GetAllSegIndexes(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	})
	segIdxes := mt.GetAllSegIndexes()
	assert.Equal(t, 1, len(segIdxes))

	err := mt.MarkSegmentsIndexAsDeleted(func(segIndex *model.SegmentIndex) bool {
		return segIndex.SegmentID == segID
	})
	assert.NoError(t, err)

	segIdxes = mt.GetAllSegIndexes()
	assert.Equal(t, 1, len(segIdxes))
}

func TestMetaTable_GetDeletedIndexes(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
		},
	})

	deletedIndexes := mt.GetDeletedIndexes()
	assert.Equal(t, 0, len(deletedIndexes))

	err := mt.MarkIndexAsDeleted(collID, []int64{indexID})
	assert.NoError(t, err)

	deletedIndexes = mt.GetDeletedIndexes()
	assert.Equal(t, 1, len(deletedIndexes))
}

func TestMetaTable_GetBuildIDsFromIndexID(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	buildIDs := mt.GetBuildIDsFromIndexID(indexID)
	assert.Equal(t, 1, len(buildIDs))
}

func TestMetaTable_GetBuildIDsFromSegIDs(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	buildIDs := mt.GetBuildIDsFromSegIDs([]int64{segID, segID + 1})
	assert.Equal(t, 1, len(buildIDs))
}

func TestMetaTable_RemoveIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				remove: func(s string) error {
					return nil
				},
			},
		})
		err := mt.RemoveIndex(collID, indexID)
		assert.NoError(t, err)
		_, ok := mt.collectionIndexes[collID][indexID]
		assert.False(t, ok)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				remove: func(s string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.RemoveIndex(collID, indexID)
		assert.Error(t, err)
		_, ok := mt.collectionIndexes[collID][indexID]
		assert.True(t, ok)
	})
}

func TestMetaTable_RemoveSegmentIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				remove: func(s string) error {
					return nil
				},
			},
		})
		err := mt.RemoveSegmentIndex(collID, partID, segID, buildID)
		assert.NoError(t, err)
		_, ok := mt.segmentIndexes[segID][indexID]
		assert.False(t, ok)
		_, ok = mt.buildID2SegmentIndex[buildID]
		assert.False(t, ok)
	})
	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				remove: func(s string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.RemoveSegmentIndex(collID, partID, segID, buildID)
		assert.Error(t, err)
		_, ok := mt.segmentIndexes[segID][indexID]
		assert.True(t, ok)
		_, ok = mt.buildID2SegmentIndex[buildID]
		assert.True(t, ok)
	})
}

func TestMetaTable_HasBuildID(t *testing.T) {
	mt := constructMetaTable(&indexcoord.Catalog{})
	has := mt.HasBuildID(buildID)
	assert.True(t, has)

	has = mt.HasBuildID(buildID + 1)
	assert.False(t, has)
}

func TestMetaTable_ResetNodeID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := mt.ResetNodeID(buildID)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), mt.buildID2SegmentIndex[buildID].NodeID)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.ResetNodeID(buildID)
		assert.Error(t, err)

		err = mt.ResetNodeID(buildID + 1)
		assert.Error(t, err)
	})
}

func TestMetaTable_ResetMeta(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := &metaTable{
			catalog: &indexcoord.Catalog{Txn: NewMockEtcdKV()},
			buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
				buildID: {
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1024,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        1,
					IndexVersion:  1,
					IndexState:    commonpb.IndexState_InProgress,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    1,
					IndexFileKeys: nil,
					IndexSize:     0,
					WriteHandoff:  false,
				},
			},
			segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
				segID: {
					indexID: {
						SegmentID:     segID,
						CollectionID:  collID,
						PartitionID:   partID,
						NumRows:       1024,
						IndexID:       indexID,
						BuildID:       buildID,
						NodeID:        1,
						IndexVersion:  1,
						IndexState:    commonpb.IndexState_InProgress,
						FailReason:    "",
						IsDeleted:     false,
						CreateTime:    1,
						IndexFileKeys: nil,
						IndexSize:     0,
						WriteHandoff:  false,
					},
				},
			},
		}
		err := mt.ResetMeta(buildID)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), mt.buildID2SegmentIndex[buildID].NodeID)
		assert.Equal(t, commonpb.IndexState_Unissued, mt.buildID2SegmentIndex[buildID].IndexState)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.ResetMeta(buildID)
		assert.Error(t, err)

		err = mt.ResetMeta(buildID + 1)
		assert.Error(t, err)
	})
}

func TestMetaTable_FinishTask(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     segID + 1,
		CollectionID:  collID,
		PartitionID:   partID,
		NumRows:       10240,
		IndexID:       indexID,
		BuildID:       buildID + 1,
		NodeID:        1,
		IndexVersion:  1,
		IndexState:    commonpb.IndexState_InProgress,
		FailReason:    "",
		IsDeleted:     false,
		CreateTime:    1234,
		IndexFileKeys: nil,
		IndexSize:     0,
	}
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				save: func(s string, s2 string) error {
					return nil
				},
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID + 1,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file3", "file4"},
			SerializedSize: 1025,
			FailReason:     "",
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Finished, mt.buildID2SegmentIndex[buildID+1].IndexState)
		assert.Equal(t, uint64(1025), mt.buildID2SegmentIndex[buildID+1].IndexSize)
		assert.ElementsMatch(t, []string{"file3", "file4"}, mt.buildID2SegmentIndex[buildID+1].IndexFileKeys)
	})

	t.Run("state failed", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				save: func(s string, s2 string) error {
					return nil
				},
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID + 1,
			State:          commonpb.IndexState_Failed,
			IndexFileKeys:  []string{},
			SerializedSize: 0,
			FailReason:     "failed",
		})

		assert.NoError(t, err)
		assert.Equal(t, commonpb.IndexState_Failed, mt.buildID2SegmentIndex[buildID+1].IndexState)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				save: func(s string, s2 string) error {
					return nil
				},
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.AddIndex(segIdx)
		assert.NoError(t, err)

		err = mt.FinishTask(&indexpb.IndexTaskInfo{
			BuildID:        buildID + 1,
			State:          commonpb.IndexState_Finished,
			IndexFileKeys:  []string{"file3", "file4"},
			SerializedSize: 1025,
			FailReason:     "",
		})
		assert.Error(t, err)
	})
}

func TestMetaTable_MarkSegmentsIndexAsDeletedByBuildID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return nil
				},
			},
		})
		err := mt.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID})
		assert.NoError(t, err)

		err = mt.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID})
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		mt := constructMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
			},
		})
		err := mt.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID})
		assert.Error(t, err)
	})
}

func TestMetaTable_GetDeletedSegmentIndexes(t *testing.T) {
	mt := createMetaTable(&indexcoord.Catalog{})

	segIndexes := mt.GetDeletedSegmentIndexes()
	assert.Equal(t, 1, len(segIndexes))
}
