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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/stretchr/testify/assert"
)

var (
	collID  = UniqueID(1)
	partID  = UniqueID(2)
	segID   = UniqueID(3)
	indexID = UniqueID(4)
	buildID = UniqueID(5)
	fieldID = UniqueID(6)

	indexName = "index"
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
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				if s == segmentIndexPrefix {
					return []string{"1"}, []string{string(value1)}, []int64{1}, 1, nil
				}
				return []string{"1"}, []string{string(value2)}, []int64{1}, 1, nil
			},
		}
		mt, err := NewMetaTable(kv)
		assert.NoError(t, err)
		assert.NotNil(t, mt)
	})

	t.Run("load collection index error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				return nil, nil, nil, 0, errors.New("error")
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})

	t.Run("load segment index error", func(t *testing.T) {
		kv := &mockETCDKV{
			loadWithRevisionAndVersions: func(s string) ([]string, []string, []int64, int64, error) {
				if s == fieldIndexPrefix {
					return []string{"1"}, []string{string(value2)}, []int64{1}, 1, nil
				}
				return nil, nil, nil, 0, errors.New("error")
			},
		}
		mt, err := NewMetaTable(kv)
		assert.Error(t, err)
		assert.Nil(t, mt)
	})
}

func constructMetaTable() *metaTable {
	return &metaTable{
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: &model.Index{
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   0,
					TypeParams:   nil,
					IndexParams:  nil,
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: &model.SegmentIndex{
					Segment: model.Segment{
						SegmentID:    segID,
						CollectionID: collID,
						PartitionID:  partID,
						NumRows:      1024,
					},
					IndexID:        indexID,
					BuildID:        buildID,
					NodeID:         0,
					IndexState:     commonpb.IndexState_Unissued,
					FailReason:     "",
					IndexVersion:   0,
					IsDeleted:      false,
					CreateTime:     0,
					IndexFilePaths: nil,
					IndexSize:      0,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				Segment: model.Segment{
					SegmentID:    segID,
					CollectionID: collID,
					PartitionID:  partID,
					NumRows:      1024,
				},
				IndexID:        indexID,
				BuildID:        buildID,
				NodeID:         0,
				IndexState:     commonpb.IndexState_Unissued,
				FailReason:     "",
				IndexVersion:   0,
				IsDeleted:      false,
				CreateTime:     0,
				IndexFilePaths: nil,
				IndexSize:      0,
			},
		},
	}
}

func TestMetaTable_GetAllIndexMeta(t *testing.T) {
	mt := constructMetaTable()

}
