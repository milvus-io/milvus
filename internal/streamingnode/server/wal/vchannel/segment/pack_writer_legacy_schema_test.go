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

package segment

import (
	"context"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A schema selected by the legacy latest fallback may contain a later nullable
// or defaulted scalar field. Its absence from the old physical groups must not
// invalidate the mapping of fields that were already persisted.
func TestCurrentSplitForGrowingPackAllowsNewScalarFields(t *testing.T) {
	for _, field := range []*schemapb.FieldSchema{
		{FieldID: 122, DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: 122, DataType: schemapb.DataType_Int64, DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{LongData: 42},
		}},
	} {
		name := "nullable"
		if field.GetDefaultValue() != nil {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			schema := testGrowingPackSchema()
			schema.Version = 2
			schema.Fields = append(schema.Fields, field)
			meta := &streamingpb.SegmentAssignmentMeta{
				SchemaVersion: 2, StorageVersion: storage.StorageV2,
				PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
					Binlogs: []*streamingpb.L1SegmentBinLogs{{FieldBinlog: []*datapb.FieldBinlog{
						{FieldID: 0, ChildFields: []int64{100, 0, 1}, Format: "parquet"},
						{FieldID: 101, ChildFields: []int64{101}, Format: "parquet"},
					}}},
				},
			}

			groups, err := currentSplitForGrowingPack(schema, nil, meta)

			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.Equal(t, []int64{100, 0, 1}, groups[0].Fields)
			require.Equal(t, []int{2, 0, 1}, groups[0].Columns)
			require.Equal(t, []int64{101}, groups[1].Fields)
			require.Equal(t, []int{3}, groups[1].Columns)
		})
	}
}

func TestFlushInsertBufferStorageV2LegacySchemaRoundTrip(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		newField *schemapb.FieldSchema
	}{
		{name: "historical version includes field 121"},
		{name: "latest adds nullable field 122", newField: &schemapb.FieldSchema{
			FieldID: 122, Name: "later", DataType: schemapb.DataType_Int64, Nullable: true,
		}},
		{name: "latest adds default field 122 preserves old columns", newField: &schemapb.FieldSchema{
			FieldID: 122, Name: "later", DataType: schemapb.DataType_Int64,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			storageConfig, cm := setupV3TestEnv(t)
			pt := paramtable.Get()
			require.NoError(t, pt.Save(pt.CommonCfg.UseLoonFFI.Key, "false"))
			const collectionID, partitionID, segmentID, timeTick = int64(1), int64(2), int64(3), uint64(30)
			schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 121, Name: "added_before_upgrade", DataType: schemapb.DataType_Int64, Nullable: true},
			}}
			if testCase.newField != nil {
				schema.Version = 2
				schema.Fields = append(schema.Fields, testCase.newField)
			}
			field121 := newTestLongFieldData(121, 11, 0, 33)
			field121.ValidData = []bool{true, false, true}
			insert := message.NewInsertMessageBuilderV1().WithVChannel("legacy_v1").
				WithHeader(&messagespb.InsertMessageHeader{
					CollectionId: collectionID,
					Partitions: []*messagespb.PartitionSegmentAssignment{{
						PartitionId: partitionID, Rows: 3,
						SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: segmentID},
					}},
				}).WithBody(&msgpb.InsertRequest{
				Version: msgpb.InsertDataVersion_ColumnBased,
				RowIDs:  []int64{1, 2, 3}, Timestamps: []uint64{999, 999, 999}, NumRows: 3,
				FieldsData: []*schemapb.FieldData{
					newTestLongFieldData(0, 1, 2, 3),
					newTestLongFieldData(1, 999, 999, 999),
					newTestLongFieldData(100, 101, 102, 103), field121,
				},
			}).MustBuildMutable().WithTimeTick(timeTick).WithLastConfirmedUseMessageID().
				IntoImmutableMessage(walimplstest.NewTestMessageID(1))
			// Use an order different from the encoding schema to catch both the
			// missing-field default-to-zero bug and incorrect restored indices.
			oldFields := []int64{100, 121, 0, 1}
			pack := &flushPack{
				Meta: &streamingpb.SegmentAssignmentMeta{
					CollectionId: collectionID, PartitionId: partitionID, SegmentId: segmentID,
					Vchannel: "legacy_v1", SchemaVersion: schema.GetVersion(), StorageVersion: storage.StorageV2,
					PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
						Binlogs: []*streamingpb.L1SegmentBinLogs{{FieldBinlog: []*datapb.FieldBinlog{{
							FieldID: 0, ChildFields: oldFields, Format: "parquet",
						}}}},
					},
				},
				CollectionID: collectionID, PartitionID: partitionID, SegmentID: segmentID,
				VChannel: "legacy_v1", FromTimeTick: timeTick, ToTimeTick: timeTick,
				Schema: schema, Rows: 3, Inserts: []message.ImmutableMessage{insert},
			}
			writer := NewBulkPackWriter(cm, allocator.NewLocalAllocator(1, math.MaxInt64), storageConfig)

			result, err := writer.FlushInsertBuffer(ctx, pack)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Len(t, result.PersistedStorage.GetBinlogs(), 1)
			binlogs := result.PersistedStorage.GetBinlogs()[0].GetFieldBinlog()
			require.Len(t, binlogs, 1)
			require.Equal(t, oldFields, binlogs[0].GetChildFields())
			require.NotContains(t, binlogs[0].GetChildFields(), int64(122))
			require.Len(t, binlogs[0].GetBinlogs(), 1)
			log := binlogs[0].GetBinlogs()[0]
			require.FileExists(t, log.GetLogPath())
			require.Equal(t, int64(3), log.GetEntriesNum())
			require.Equal(t, int64(1), log.GetFieldNullCounts()[121])
			require.NotNil(t, result.PersistedStorage.GetStatistics())
			require.Equal(t, int64(1), result.PersistedStorage.GetStatistics().GetNullCounts()[121])

			readSchema := schema
			if testCase.newField != nil && !testCase.newField.GetNullable() {
				// The Go binlog reader rejects an absent non-nullable field even
				// when it has a default. Verify the actual stored fields here;
				// default materialization during sealed loading is a separate path.
				readSchema = &schemapb.CollectionSchema{Version: 1, Fields: schema.Fields[:4]}
			}
			reader, err := storage.NewBinlogRecordReader(ctx, binlogs, readSchema,
				storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(storageConfig))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			record, err := reader.Next()
			require.NoError(t, err)
			require.Equal(t, 3, record.Len())
			values := record.Column(121).(*array.Int64)
			require.Equal(t, int64(11), values.Value(0))
			require.True(t, values.IsNull(1))
			require.Equal(t, int64(33), values.Value(2))
			require.Equal(t, 1, values.NullN())
			require.Equal(t, []int64{101, 102, 103}, record.Column(100).(*array.Int64).Int64Values())
			require.Equal(t, []int64{1, 2, 3}, record.Column(0).(*array.Int64).Int64Values())
			require.Equal(t, []int64{30, 30, 30}, record.Column(1).(*array.Int64).Int64Values())
			if testCase.newField != nil && testCase.newField.GetNullable() {
				require.Equal(t, 3, record.Column(122).(*array.Int64).NullN())
			}
			_, err = reader.Next()
			require.ErrorIs(t, err, io.EOF)
		})
	}
}
