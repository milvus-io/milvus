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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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

func createIndexMeta(catalog metastore.DataCoordCatalog) *IndexMeta {
	indexBuildInfo := NewSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1025,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
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
		SegmentID:           meta.SegID + 1,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 1,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID + 2,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 2,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID + 3,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 3,
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
		SegmentID:           meta.SegID + 4,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 4,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID + 5,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 5,
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
		SegmentID:           meta.SegID + 6,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 6,
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
		SegmentID:           meta.SegID + 7,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 7,
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
		SegmentID:           meta.SegID + 8,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 8,
		NodeID:              meta.NodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           meta.SegID + 9,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 9,
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
		SegmentID:           meta.SegID + 10,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 10,
		NodeID:              meta.NodeID,
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
	segIdx0.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1025,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID,
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
	segIdx1.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 1,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 1,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 2,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 2,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx3 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx3.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 3,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 3,
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
	segIdx4.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 4,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 4,
		NodeID:              meta.NodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx5 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx5.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 5,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 5,
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
	segIdx6.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 6,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 6,
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
	segIdx7.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 7,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 7,
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
	segIdx8.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 8,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             1026,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 8,
		NodeID:              meta.NodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx9 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx9.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 9,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 9,
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
	segIdx10.Insert(meta.IndexID, &model.SegmentIndex{
		SegmentID:           meta.SegID + 10,
		CollectionID:        meta.CollID,
		PartitionID:         meta.PartID,
		NumRows:             500,
		IndexID:             meta.IndexID,
		BuildID:             meta.BuildID + 10,
		NodeID:              meta.NodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes.Insert(meta.SegID, segIdx0)
	segIndexes.Insert(meta.SegID+1, segIdx1)
	segIndexes.Insert(meta.SegID+2, segIdx2)
	segIndexes.Insert(meta.SegID+3, segIdx3)
	segIndexes.Insert(meta.SegID+4, segIdx4)
	segIndexes.Insert(meta.SegID+5, segIdx5)
	segIndexes.Insert(meta.SegID+6, segIdx6)
	segIndexes.Insert(meta.SegID+7, segIdx7)
	segIndexes.Insert(meta.SegID+8, segIdx8)
	segIndexes.Insert(meta.SegID+9, segIdx9)
	segIndexes.Insert(meta.SegID+10, segIdx10)

	return &IndexMeta{
		Catalog: catalog,
		KeyLock: lock.NewKeyLock[UniqueID](),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
			meta.CollID: {
				meta.IndexID: {
					TenantID:     "",
					CollectionID: meta.CollID,
					FieldID:      meta.FieldID,
					IndexID:      meta.IndexID,
					IndexName:    meta.IndexName,
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
		SegmentIndexes:   segIndexes,
		SegmentBuildInfo: indexBuildInfo,
	}
}

func createIndexMetaWithSegment(catalog metastore.DataCoordCatalog,
	collID, partID, segID, indexID, fieldID, buildID UniqueID,
) *IndexMeta {
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
	im := &IndexMeta{
		Catalog: catalog,
		KeyLock: lock.NewKeyLock[UniqueID](),
		Indexes: map[UniqueID]map[UniqueID]*model.Index{
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
		SegmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		SegmentBuildInfo: NewSegmentIndexBuildInfo(),
	}
	im.SegmentIndexes.Insert(segID, tasks)
	im.SegmentBuildInfo.Add(indexTask)
	return im
}

// setupManifestReloadMeta builds a meta holding one healthy StorageV3 segment
// and the collection's index definition, but no SegmentIndex record - the
// state a restart lands in after manifest publication retires the Finished
// etcd row.
func setupManifestReloadMeta(t *testing.T) (*meta.Meta, *IndexTestMetas) {
	meta.MockManifestIndexSubmissions(t)
	t.Helper()
	const (
		collID  = UniqueID(100)
		partID  = UniqueID(10)
		segID   = UniqueID(5001)
		fieldID = UniqueID(101)
		indexID = UniqueID(500)
	)
	m, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.TODO(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID:               segID,
		CollectionID:     collID,
		PartitionID:      partID,
		State:            commonpb.SegmentState_Flushed,
		NumOfRows:        100,
		StorageVersion:   storage.StorageV3,
		ManifestPath:     packed.MarshalManifestPath("/tmp/test-reload/insert_log/100/10/5001", 3),
		ManifestHasIndex: true,
	})))
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath("/tmp/test-reload"))
	// Index definitions stay in etcd regardless of the switch: a manifest
	// cannot record "the user asked for an HNSW index on this field".
	require.NoError(t, dm.IndexMeta.CreateIndex(context.TODO(), &model.Index{
		CollectionID: collID,
		FieldID:      fieldID,
		IndexID:      indexID,
		IndexName:    "idx",
	}))
	return m, dm
}
