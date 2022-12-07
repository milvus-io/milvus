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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/stretchr/testify/assert"
)

func Test_createIndexAtomic(t *testing.T) {
	meta := &metaTable{
		catalog: &indexcoord.Catalog{Txn: &mockETCDKV{
			save: func(s string, s2 string) error {
				return errors.New("error")
			},
		}},
		indexLock:         sync.RWMutex{},
		segmentIndexLock:  sync.RWMutex{},
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: &model.SegmentIndex{
					SegmentID:     segID,
					CollectionID:  collID,
					PartitionID:   partID,
					NumRows:       1025,
					IndexID:       indexID,
					BuildID:       buildID,
					NodeID:        nodeID,
					IndexVersion:  0,
					IndexState:    commonpb.IndexState_Unissued,
					FailReason:    "",
					IsDeleted:     false,
					CreateTime:    0,
					IndexFileKeys: nil,
					IndexSize:     0,
					WriteHandoff:  false,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:     segID,
				CollectionID:  collID,
				PartitionID:   partID,
				NumRows:       1025,
				IndexID:       indexID,
				BuildID:       buildID,
				NodeID:        nodeID,
				IndexVersion:  0,
				IndexState:    commonpb.IndexState_Unissued,
				FailReason:    "",
				IsDeleted:     false,
				CreateTime:    0,
				IndexFileKeys: nil,
				IndexSize:     0,
				WriteHandoff:  false,
			},
		},
	}
	ic := &IndexCoord{
		metaTable: meta,
	}
	cit := &CreateIndexTask{
		BaseTask: BaseTask{
			table: meta,
		},
		indexCoordClient: ic,
		indexID:          indexID,
		req: &indexpb.CreateIndexRequest{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexName:    indexName,
			Timestamp:    createTs,
		},
	}

	index := &model.Index{
		TenantID:        "",
		CollectionID:    collID,
		FieldID:         fieldID,
		IndexID:         indexID,
		IndexName:       indexName,
		IsDeleted:       false,
		CreateTime:      createTs,
		TypeParams:      nil,
		IndexParams:     nil,
		IsAutoIndex:     false,
		UserIndexParams: nil,
	}
	segmentsInfo := []*datapb.SegmentInfo{
		{
			ID:                   segID,
			CollectionID:         collID,
			PartitionID:          partID,
			InsertChannel:        "",
			NumOfRows:            1025,
			State:                commonpb.SegmentState_Flushed,
			MaxRowNum:            65535,
			LastExpireTime:       0,
			StartPosition:        nil,
			DmlPosition:          nil,
			Binlogs:              nil,
			Statslogs:            nil,
			Deltalogs:            nil,
			CreatedByCompaction:  false,
			CompactionFrom:       nil,
			DroppedAt:            0,
			IsImporting:          false,
			IsFake:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
	}

	buildIDs, segs, err := cit.createIndexAtomic(index, segmentsInfo)
	// index already exist
	assert.Equal(t, 0, len(buildIDs))
	assert.Equal(t, 0, len(segs))
	assert.Error(t, err)
}
