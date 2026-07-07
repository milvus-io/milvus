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

package segments

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestNewGrowingSegmentWithCreationSchema simulates the WAL-replay scenario of
// issue #51117: the collection schema has dropped a field, while the replayed
// insert payload still carries its column. A growing segment created with the
// payload's era schema accepts the payload; one created with the current schema
// rejects it (segcore unknown-field invariant).
func TestNewGrowingSegmentWithCreationSchema(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(t.Name())
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	const (
		collectionID   = int64(200)
		partitionID    = int64(20)
		droppedFieldID = int64(199)
		msgLength      = 8
	)

	// Current schema: the era field has been dropped since.
	currentSchema := mock_segcore.GenTestCollectionSchema("test-era-creation", schemapb.DataType_Int64, true)
	// Era schema: current schema plus the since-dropped VarChar field.
	eraSchema := proto.Clone(currentSchema).(*schemapb.CollectionSchema)
	eraSchema.Fields = append(eraSchema.Fields, &schemapb.FieldSchema{
		FieldID:  droppedFieldID,
		Name:     "dropped_field",
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "64"},
		},
	})

	manager := NewManager()
	err := manager.Collection.PutOrRef(collectionID, currentSchema,
		mock_segcore.GenTestIndexMeta(collectionID, currentSchema),
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: collectionID,
			PartitionIDs: []int64{partitionID},
		})
	require.NoError(t, err)
	collection := manager.Collection.Get(collectionID)
	defer DeleteCollection(collection)

	newLoadInfo := func(segmentID int64) *querypb.SegmentLoadInfo {
		return &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID),
			Level:         datapb.SegmentLevel_L1,
		}
	}
	newEraPayload := func(segmentID int64) ([]int64, []uint64, *segcorepb.InsertRecord) {
		insertMsg, err := mock_segcore.GenInsertMsg(collection.GetCCollection(), partitionID, segmentID, msgLength)
		require.NoError(t, err)
		droppedData := make([]string, msgLength)
		for i := range droppedData {
			droppedData[i] = fmt.Sprintf("dropped-%d", i)
		}
		insertMsg.FieldsData = append(insertMsg.FieldsData, &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldName: "dropped_field",
			FieldId:   droppedFieldID,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{Data: droppedData},
					},
				},
			},
		})
		insertRecord, err := storage.TransferInsertMsgToInsertRecord(eraSchema, insertMsg)
		require.NoError(t, err)
		return insertMsg.RowIDs, insertMsg.Timestamps, insertRecord
	}

	// A segment created with the era schema owns the dropped field's column and
	// accepts the replayed payload.
	eraSegment, err := NewSegment(ctx, collection, manager.Segment, SegmentTypeGrowing, 0,
		newLoadInfo(1), WithCreationSchema(eraSchema))
	require.NoError(t, err)
	defer eraSegment.Release(ctx)
	rowIDs, timestamps, insertRecord := newEraPayload(eraSegment.ID())
	require.NoError(t, eraSegment.Insert(ctx, rowIDs, timestamps, insertRecord))

	// A segment created with the current schema has no such column and must
	// reject the same payload (segcore unknown-field invariant).
	currentSegment, err := NewSegment(ctx, collection, manager.Segment, SegmentTypeGrowing, 0,
		newLoadInfo(2))
	require.NoError(t, err)
	defer currentSegment.Release(ctx)
	rowIDs, timestamps, insertRecord = newEraPayload(currentSegment.ID())
	require.Error(t, currentSegment.Insert(ctx, rowIDs, timestamps, insertRecord))
}
