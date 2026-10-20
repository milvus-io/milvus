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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func createIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	indexBuildInfo := newSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx3 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx3.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx4 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx4.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx5 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx5.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx6 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx6.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx7 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx7.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx8 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx8.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx9 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx9.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx10 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx10.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	segIndexes.Insert(segID+2, segIdx2)
	segIndexes.Insert(segID+3, segIdx3)
	segIndexes.Insert(segID+4, segIdx4)
	segIndexes.Insert(segID+5, segIdx5)
	segIndexes.Insert(segID+6, segIdx6)
	segIndexes.Insert(segID+7, segIdx7)
	segIndexes.Insert(segID+8, segIdx8)
	segIndexes.Insert(segID+9, segIdx9)
	segIndexes.Insert(segID+10, segIdx10)

	return &indexMeta{
		catalog: catalog,
		keyLock: lock.NewKeyLock[UniqueID](),
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.IndexTypeKey,
							Value: "HNSW",
						},
						{
							Key:   common.MetricTypeKey,
							Value: "L2",
						},
					},
				},
			},
		},
		segmentIndexes:   segIndexes,
		segmentBuildInfo: indexBuildInfo,
	}
}

func createIndexMetaWithSegment(catalog metastore.DataCoordCatalog,
	collID, partID, segID, indexID, fieldID, buildID UniqueID,
) *indexMeta {
	tasks := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	indexTask := &model.SegmentIndex{
		CollectionID: collID,
		PartitionID:  partID,
		SegmentID:    segID,
		IndexID:      indexID,
		BuildID:      buildID,
		IndexState:   commonpb.IndexState_Unissued,
		NodeID:       0,
		FailReason:   "",
		NumRows:      1025,
	}
	tasks.Insert(indexID, indexTask)
	im := &indexMeta{
		catalog: catalog,
		keyLock: lock.NewKeyLock[UniqueID](),
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {indexID: {
				IndexID:   indexID,
				FieldID:   fieldID,
				IndexName: "default_index",
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "index_type", Value: "HNSW"},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			}},
		},
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
	}
	im.segmentIndexes.Insert(segID, tasks)
	im.segmentBuildInfo.Add(indexTask)
	return im
}

// setupManifestReloadMeta builds a meta holding one healthy StorageV3 segment
// and the collection's index definition, but no SegmentIndex record - the
// state a restart lands in after manifest publication retires the Finished
// etcd row.
func setupManifestReloadMeta(t *testing.T) (*meta, *indexTestMetas) {
	mockManifestIndexSubmissions(t)
	t.Helper()
	const (
		collID  = UniqueID(100)
		partID  = UniqueID(10)
		segID   = UniqueID(5001)
		fieldID = UniqueID(101)
		indexID = UniqueID(500)
	)
	m, dm, err := newMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segID,
		CollectionID:     collID,
		PartitionID:      partID,
		State:            commonpb.SegmentState_Flushed,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath("/tmp/test-reload/insert_log/100/10/5001", 3),
		ManifestHasIndex: true,
	})))
	m.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-reload"))
	// Index definitions stay in etcd regardless of the switch: a manifest
	// cannot record "the user asked for an HNSW index on this field".
	require.NoError(t, dm.indexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "idx",
	}))
	return m, dm
}
