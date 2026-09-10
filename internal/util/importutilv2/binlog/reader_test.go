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

package binlog

import (
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotDeleteMapBudget(t *testing.T) {
	r := &reader{deleteData: make(map[any]typeutil.Timestamp), deleteBudget: 260}
	require.NoError(t, r.mergeSnapshotDelete(int64(1), 300))
	require.NoError(t, r.mergeSnapshotDelete(int64(1), 200))
	require.EqualValues(t, 300, r.deleteData[int64(1)])
	buffer := []byte("key")
	borrowed := unsafe.String(unsafe.SliceData(buffer), len(buffer))
	require.NoError(t, r.mergeSnapshotDelete(borrowed, 100))
	require.NoError(t, r.mergeSnapshotDelete(borrowed, 200))
	buffer[0] = 'x'
	require.EqualValues(t, 200, r.deleteData["key"], "updating an equal key must not retain borrowed Arrow memory")
	require.EqualValues(t, 259, r.deleteBytes, "replacement must not charge another key")
	require.ErrorIs(t, r.mergeSnapshotDelete(int64(2), 100), merr.ErrServiceResourceInsufficient)
	require.Len(t, r.deleteData, 2)
}

func TestSnapshotDeleteReadErrors(t *testing.T) {
	type failingDeltaReader struct{ storage.RecordReader }
	for _, mode := range []string{"open", "read", "terminal_after_record", "bad_timestamp", "bad_pk", "cancel_after_record", "cancel_before_read", "missing_pk", "zero_budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &reader{ctx: ctx, snapshotSource: &internalpb.SnapshotImportSource{}, deleteBudget: 1024,
				schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}}
			if mode == "missing_pk" {
				r.schema.Fields = nil
			}
			if mode == "zero_budget" {
				r.deleteBudget = 0
			}
			pkType, tsType := arrow.DataType(arrow.PrimitiveTypes.Int64), arrow.DataType(arrow.PrimitiveTypes.Int64)
			if mode == "bad_pk" {
				pkType = arrow.BinaryTypes.String
			}
			if mode == "bad_timestamp" {
				tsType = arrow.BinaryTypes.String
			}
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "pk", Type: pkType}, {Name: "ts", Type: tsType}}, nil))
			for i := 0; i < 2; i++ {
				if b, ok := builder.Field(i).(*array.Int64Builder); ok {
					b.Append(1)
				} else {
					builder.Field(i).(*array.StringBuilder).Append("bad")
				}
			}
			record := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{0: 0, 1: 1})
			builder.Release()
			defer record.Release()
			cause := merr.ErrIoKeyNotFound
			reads, opens, closes := 0, 0, 0
			openPatch := mockey.Mock(storage.NewDeltalogReader).To(func(context.Context, schemapb.DataType, []string, ...storage.RwOption) (storage.RecordReader, error) {
				opens++
				if mode == "open" {
					return nil, cause
				}
				return &failingDeltaReader{}, nil
			}).Build()
			defer openPatch.UnPatch()
			nextPatch := mockey.Mock((*failingDeltaReader).Next).To(func(*failingDeltaReader) (storage.Record, error) {
				reads++
				if mode == "read" || reads > 1 {
					return nil, cause
				}
				if mode == "cancel_after_record" {
					cancel()
				}
				return record, nil
			}).Build()
			defer nextPatch.UnPatch()
			closePatch := mockey.Mock((*failingDeltaReader).Close).To(func(*failingDeltaReader) error { closes++; return nil }).Build()
			defer closePatch.UnPatch()
			var err error
			if mode == "cancel_before_read" {
				cancel()
				_, err = r.mergeSnapshotDeleteFile("delta", schemapb.DataType_Int64, 0, math.MaxUint64, storage.StorageV3)
			} else {
				_, err = r.readSnapshotDeletes([]string{"delta"}, 0, math.MaxUint64, true)
			}
			require.Error(t, err)
			switch mode {
			case "open", "read":
				require.ErrorIs(t, err, cause)
				require.Equal(t, 2, opens)
			case "terminal_after_record":
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, opens)
			case "cancel_after_record", "cancel_before_read":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, opens)
			case "zero_budget":
				require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
				require.Equal(t, 1, opens)
			case "bad_pk", "bad_timestamp":
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.Equal(t, 1, opens)
			}
			if mode != "open" {
				require.Equal(t, opens, closes)
			}
		})
	}
}

func TestSnapshotL0RealFiles(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			root := t.TempDir()
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			cm := storage.NewLocalChunkManager()
			schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
			}})
			// Use real storage writers and readers. The data manifest has no own
			// deltas: successful filtering must come from the attached L0 files.
			writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, 20, schema, allocator.NewLocalAllocator(1, 1000), 1024*1024, 100,
				storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg),
				storage.WithColumnGroups([]storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{100, 0, 1}}}),
				storage.WithUploader(cm.MultiWrite))
			require.NoError(t, err)
			var values []*storage.Value
			for i, row := range [][2]int64{{1, 100}, {1, 300}, {2, 100}, {3, 300}, {4, 100}} {
				var pk any = row[0]
				if pkType == schemapb.DataType_VarChar {
					pk = fmt.Sprint(row[0])
				}
				values = append(values, &storage.Value{Value: map[int64]any{100: pk, 0: int64(i + 1), 1: row[1]}})
			}
			record, err := storage.ValueSerializer(values, schema)
			require.NoError(t, err)
			require.NoError(t, writer.Write(record))
			record.Release()
			require.NoError(t, writer.Close())
			_, _, _, manifest, _ := writer.GetLogs()
			require.NotEmpty(t, manifest)
			paths := []string{path.Join(root, "legacy.delta"), path.Join(root, "packed.delta")}
			for i, version := range []int64{storage.StorageV1, storage.StorageV2} {
				deltaWriter, err := storage.NewDeltalogWriter(ctx, 1, 10, 30, int64(i+1), pkType, paths[i],
					storage.WithVersion(version), storage.WithStorageConfig(cfg), storage.WithUploader(cm.MultiWrite))
				require.NoError(t, err)
				var arrowPK arrow.DataType = arrow.PrimitiveTypes.Int64
				if pkType == schemapb.DataType_VarChar {
					arrowPK = arrow.BinaryTypes.String
				}
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
					{Name: "pk", Type: arrowPK}, {Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				}, nil))
				rows := [][][2]int64{{{1, 200}, {2, 100}}, {{1, 150}, {1, 500}, {3, 400}}}[i]
				for _, row := range rows {
					if pkType == schemapb.DataType_Int64 {
						builder.Field(0).(*array.Int64Builder).Append(row[0])
					} else {
						builder.Field(0).(*array.StringBuilder).Append(fmt.Sprint(row[0]))
					}
					builder.Field(1).(*array.Int64Builder).Append(row[1])
				}
				deltaRecord := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{0: 0, 1: 1})
				builder.Release()
				require.NoError(t, deltaWriter.Write(deltaRecord))
				deltaRecord.Release()
				require.NoError(t, deltaWriter.Close())
			}
			for _, commit := range []uint64{0, 300} {
				source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, SourceCommitTimestamp: commit,
					LegacyL0Deltalogs: paths[:1], ManifestL0Deltalogs: paths[1:]}
				// Re-open twice to model the identical persisted input used by
				// PreImport and Import; exact row IDs prove reinsert ordering.
				for phase := 0; phase < 2; phase++ {
					r, err := NewStorageV3ManifestReader(ctx, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 512)
					require.NoError(t, err)
					var ids []int64
					for {
						batch, err := r.Read()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						for i := 0; i < batch.GetRowNum(); i++ {
							ids = append(ids, batch.Data[common.RowIDField].GetRow(i).(int64))
						}
					}
					want := []int64{2, 3, 5}
					if commit != 0 {
						want = []int64{1, 2, 3, 5}
					}
					require.Equal(t, want, ids)
					require.LessOrEqual(t, r.deleteBytes, int64(512))
					r.Close()
					require.Nil(t, r.deleteData)
				}
			}
			source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest,
				LegacyL0Deltalogs: paths, ManifestL0Deltalogs: nil}
			// V2 metadata can reference either legacy or packed files; the
			// legacy inventory must retain its established V1/V2 fallback.
			r, err := NewStorageV3ManifestReader(ctx, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 512)
			require.NoError(t, err)
			r.Close()
			_, err = NewStorageV3ManifestReader(ctx, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 0)
			require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
			_, err = NewStorageV3ManifestReader(ctx, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 1)
			require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = NewStorageV3ManifestReader(canceled, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 512)
			require.Error(t, err)
			canceled, cancel = context.WithCancel(ctx)
			defer cancel()
			r, err = NewStorageV3ManifestReader(canceled, cm, schema, cfg, manifest, 0, 450, 1024, "", source, 512)
			require.NoError(t, err)
			cancel()
			_, err = r.Read()
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, r.dr)
			require.Nil(t, r.deleteData)
		})
	}
}

const (
	insertPrefix = "mock-insert-binlog-prefix"
	deltaPrefix  = "mock-delta-binlog-prefix"
)

type ReaderSuite struct {
	suite.Suite

	schema  *schemapb.CollectionSchema
	numRows int

	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType

	deletePKs []storage.PrimaryKey
	deleteTss []int64

	tsStart uint64
	tsEnd   uint64
}

type storageV3DeltaRecordReader struct {
	record storage.Record
	read   bool
}

func (r *storageV3DeltaRecordReader) Next() (storage.Record, error) {
	if r.read {
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
		return nil, io.EOF
	}
	r.read = true
	return r.record, nil
}

func (r *storageV3DeltaRecordReader) Close() error {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	return nil
}

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {
	// default suite params
	suite.numRows = 100
	suite.tsStart = 0
	suite.tsEnd = math.MaxUint64
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func genBinlogPath(fieldID int64) string {
	return fmt.Sprintf("backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/%d/6666", fieldID)
}

func genBinlogPaths(fieldIDs []int64) map[int64][]string {
	binlogPaths := make(map[int64][]string)
	for _, fieldID := range fieldIDs {
		binlogPaths[fieldID] = []string{genBinlogPath(fieldID)}
	}
	return binlogPaths
}

func createBinlogBuf(t *testing.T, field *schemapb.FieldSchema, data storage.FieldData) []byte {
	dataType := field.GetDataType()
	w := storage.NewInsertBinlogWriter(dataType, 1, 1, 1, field.GetFieldID(), field.GetNullable())
	assert.NotNil(t, w)
	defer w.Close()

	var dim int64
	var err error
	dim, err = typeutil.GetDim(field)
	if err != nil || dim == 0 {
		dim = 1
	}

	evt, err := w.NextInsertEventWriter(storage.WithDim(int(dim)), storage.WithNullable(field.GetNullable()), storage.WithElementType(field.GetElementType()))
	assert.NoError(t, err)

	evt.SetEventTimestamp(1, math.MaxInt64)
	w.SetEventTimeStamp(1, math.MaxInt64)

	// without the two lines, the case will crash at here.
	// the "original_size" is come from storage.originalSizeKey
	sizeTotal := data.GetMemorySize()
	w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.(*storage.BoolFieldData).Data, data.(*storage.BoolFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.(*storage.Int8FieldData).Data, data.(*storage.Int8FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.(*storage.Int16FieldData).Data, data.(*storage.Int16FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.(*storage.Int32FieldData).Data, data.(*storage.Int32FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.(*storage.Int64FieldData).Data, data.(*storage.Int64FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.(*storage.FloatFieldData).Data, data.(*storage.FloatFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.(*storage.DoubleFieldData).Data, data.(*storage.DoubleFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_VarChar:
		values := data.(*storage.StringFieldData).Data
		validValues := data.(*storage.StringFieldData).ValidData
		for i, val := range values {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneStringToPayload(val, valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_JSON:
		rows := data.(*storage.JSONFieldData).Data
		validValues := data.(*storage.JSONFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneJSONToPayload(rows[i], valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_Array:
		rows := data.(*storage.ArrayFieldData).Data
		validValues := data.(*storage.ArrayFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneArrayToPayload(rows[i], valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_BinaryVector:
		vectors := data.(*storage.BinaryVectorFieldData).Data
		err = evt.AddBinaryVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_FloatVector:
		vectors := data.(*storage.FloatVectorFieldData).Data
		err = evt.AddFloatVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_Float16Vector:
		vectors := data.(*storage.Float16VectorFieldData).Data
		err = evt.AddFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_BFloat16Vector:
		vectors := data.(*storage.BFloat16VectorFieldData).Data
		err = evt.AddBFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_SparseFloatVector:
		vectors := data.(*storage.SparseFloatVectorFieldData)
		err = evt.AddSparseFloatVectorToPayload(vectors)
		assert.NoError(t, err)
	case schemapb.DataType_Int8Vector:
		vectors := data.(*storage.Int8VectorFieldData).Data
		err = evt.AddInt8VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_ArrayOfVector:
		elementType := field.GetElementType()
		switch elementType {
		case schemapb.DataType_FloatVector:
			vectors := data.(*storage.VectorArrayFieldData)
			err = evt.AddVectorArrayFieldDataToPayload(vectors)
			assert.NoError(t, err)
		default:
			assert.True(t, false)
			return nil
		}
	default:
		assert.True(t, false)
		return nil
	}

	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	return buf
}

func createDeltaBuf(t *testing.T, deletePKs []storage.PrimaryKey, deleteTss []int64) []byte {
	assert.Equal(t, len(deleteTss), len(deletePKs))
	deleteData := storage.NewDeleteData(nil, nil)
	for i := range deletePKs {
		deleteData.Append(deletePKs[i], uint64(deleteTss[i]))
	}
	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 1, 1, deleteData)
	assert.NoError(t, err)
	return blob.Value
}

func (suite *ReaderSuite) createMockChunk(schema *schemapb.CollectionSchema, insertBinlogs map[int64][]string, expectRead bool) (*mocks.ChunkManager, *storage.InsertData) {
	var deltaLogs []string
	if len(suite.deletePKs) != 0 {
		deltaLogs = []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
		}
	}

	cm := mocks.NewChunkManager(suite.T())

	originalInsertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)

	insertLogs := lo.Flatten(lo.Values(insertBinlogs))

	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			for _, filePath := range insertLogs {
				if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
					return nil
				}
			}
			return nil
		})

	if expectRead {
		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range deltaLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		allFields := typeutil.GetAllFieldSchemas(schema)
		for _, field := range allFields {
			fieldID := field.GetFieldID()
			logs, ok := insertBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, insertBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					originalInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, originalInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		if len(suite.deletePKs) != 0 {
			for _, path := range deltaLogs {
				buf := createDeltaBuf(suite.T(), suite.deletePKs, suite.deleteTss)
				cm.EXPECT().MultiRead(mock.Anything, []string{path}).Return([][]byte{buf}, nil)
			}
		}
	}

	return cm, originalInsertData
}

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      int64(common.RowIDField),
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      int64(common.TimeStampField),
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: suite.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dataType.String(),
				DataType:    dataType,
				ElementType: elemType,
				Nullable:    nullable,
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 103,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     104,
						Name:        "struct_str",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_VarChar,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxLengthKey,
								Value: "256",
							},
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
						},
					},
					{
						FieldID:     105,
						Name:        "struct_float_vector",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
							{
								Key:   common.DimKey,
								Value: "8",
							},
						},
					},
				},
			},
		},
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	insertBinlogs := genBinlogPaths(lo.Map(allFields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))
	cm, originalInsertData := suite.createMockChunk(schema, insertBinlogs, true)
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(128, nil)

	reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.NoError(err)
	insertData, err := reader.Read()
	suite.NoError(err)
	size, err := reader.Size()
	suite.NoError(err)
	suite.Equal(int64(128*len(lo.Flatten(lo.Values(insertBinlogs)))), size)
	size2, err := reader.Size() // size is cached
	suite.NoError(err)
	suite.Equal(size, size2)

	pks, err := storage.GetPkFromInsertData(schema, originalInsertData)
	suite.NoError(err)
	tss, err := storage.GetTimestampFromInsertData(originalInsertData)
	suite.NoError(err)
	expectInsertData, err := storage.NewInsertData(schema)
	suite.NoError(err)
	for _, field := range schema.GetFields() {
		expectInsertData.Data[field.GetFieldID()], err = storage.NewFieldData(field.GetDataType(), field, suite.numRows)
		suite.NoError(err)
	}
OUTER:
	for i := 0; i < suite.numRows; i++ {
		if uint64(tss.Data[i]) < suite.tsStart || uint64(tss.Data[i]) > suite.tsEnd {
			continue
		}
		for j := 0; j < len(suite.deletePKs); j++ {
			if suite.deletePKs[j].GetValue() == pks.GetRow(i) && suite.deleteTss[j] > tss.Data[i] {
				continue OUTER
			}
		}
		err = expectInsertData.Append(originalInsertData.GetRow(i))
		suite.NoError(err)
	}

	expectRowCount := expectInsertData.GetRowNum()
	for fieldID, data := range insertData.Data {
		suite.Equal(expectRowCount, data.RowNum())
		fieldData := expectInsertData.Data[fieldID]
		fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
		for i := 0; i < expectRowCount; i++ {
			expect := fieldData.GetRow(i)
			actual := data.GetRow(i)
			switch fieldDataType {
			case schemapb.DataType_Array:
				if expect == nil {
					suite.Nil(expect)
				} else {
					suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				}
			case schemapb.DataType_ArrayOfVector:
				suite.True(slices.Equal(expect.(*schemapb.VectorField).GetFloatVector().GetData(), actual.(*schemapb.VectorField).GetFloatVector().GetData()))
			default:
				suite.Equal(expect, actual)
			}
		}
	}
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, false)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, false)

	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, true)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, true)
}

func (suite *ReaderSuite) TestWithTSRangeAndDelete() {
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(4),
		storage.NewInt64PrimaryKey(6),
		storage.NewInt64PrimaryKey(8),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewVarCharPrimaryKey("1"),
		storage.NewVarCharPrimaryKey("4"),
		storage.NewVarCharPrimaryKey("6"),
		storage.NewVarCharPrimaryKey("8"),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestVector() {
	suite.pkDataType = schemapb.DataType_Int64
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(4),
		storage.NewInt64PrimaryKey(6),
		storage.NewInt64PrimaryKey(8),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_SparseFloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Int8Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestVerify() {
	suite.deletePKs = []storage.PrimaryKey{}

	pkFieldID := int64(100)
	vecFieldID := int64(101)
	nullableFieldID := int64(102)
	functionFieldID := int64(103)
	dynamicFieldID := int64(104)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:  nullableFieldID,
				Name:     "nullable",
				DataType: schemapb.DataType_Double,
				Nullable: true,
			},
			{
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}
	insertBinlogs := genBinlogPaths(lo.Map(schema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))

	checkFunc := func() {
		cm, _ := suite.createMockChunk(schema, insertBinlogs, false)
		reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
		suite.Error(err)
		suite.Nil(reader)
	}

	// no insert binlogs to import
	reader, err := NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.Error(err)
	suite.Nil(reader)

	// too many input paths
	reader, err = NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix, "dummy"}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.Error(err)
	suite.Nil(reader)

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{}
	checkFunc()

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{genBinlogPath(common.RowIDField)}
	insertBinlogs[common.TimeStampField] = []string{}
	checkFunc()

	// binlog count not equal
	insertBinlogs[common.TimeStampField] = []string{genBinlogPath(common.TimeStampField)}
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID), genBinlogPath(vecFieldID)}
	checkFunc()

	// vector field is required
	insertBinlogs[vecFieldID] = []string{}
	checkFunc()

	// primary key is required
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID)}
	insertBinlogs[pkFieldID] = []string{}
	checkFunc()

	// function output field is required
	insertBinlogs[pkFieldID] = []string{genBinlogPath(pkFieldID)}
	insertBinlogs[functionFieldID] = []string{}
	checkFunc()
}

func (suite *ReaderSuite) TestZeroDeltaRead() {
	suite.deletePKs = []storage.PrimaryKey{}

	mockChunkFunc := func(sourceSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) *mocks.ChunkManager {
		sourceBinlogs := genBinlogPaths(lo.Map(sourceSchema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
			return fieldSchema.GetFieldID()
		}))

		cm := mocks.NewChunkManager(suite.T())

		sourceInsertData, err := testutil.CreateInsertData(sourceSchema, suite.numRows)
		suite.NoError(err)

		sourceInsertLogs := lo.Flatten(lo.Values(sourceBinlogs))

		cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range sourceInsertLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		for _, field := range sourceSchema.Fields {
			fieldID := field.GetFieldID()
			logs, ok := expectReadBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, expectReadBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					sourceInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, sourceInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				return nil
			})

		return cm
	}

	rowID := int64(common.RowIDField)
	tsID := int64(common.TimeStampField)
	pkFieldID := int64(100)
	vecFieldID := int64(101)
	functionFieldID := int64(102)
	nullableFieldID := int64(103)
	dynamicFieldID := int64(104)
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      rowID,
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      tsID,
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:      nullableFieldID,
				Name:         "nullable",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}

	checkFunc := func(targetSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) {
		cm := mockChunkFunc(sourceSchema, expectReadBinlogs)
		reader, err := NewReader(context.Background(), cm, targetSchema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
		suite.NoError(err)
		suite.NotNil(reader)

		readData, err := reader.Read()
		suite.NoError(err)
		suite.Equal(suite.numRows, readData.GetRowNum())

		for _, field := range targetSchema.Fields {
			fieldID := field.GetFieldID()
			fieldData, ok := readData.Data[fieldID]
			if !ok {
				// if this field has no data, it must be nullable/default or dynamic
				suite.True(field.GetIsDynamic() || field.GetNullable() || field.GetDefaultValue() != nil)
			} else {
				suite.Equal(suite.numRows, fieldData.RowNum())
			}
		}
	}

	targetSchemaFunc := func(from int, to int, newFields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, sourceSchema.Fields[from:to]...)
		fields = append(fields, newFields...)
		return &schemapb.CollectionSchema{Fields: fields}
	}

	// the target schema lacks some fields(not required field), can import
	checkFunc(targetSchemaFunc(0, 3), map[int64][]string{
		rowID:     {genBinlogPath(rowID)},
		tsID:      {genBinlogPath(tsID)},
		pkFieldID: {genBinlogPath(pkFieldID)},
	})

	// the target schema has a new nullable field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "new",
		DataType: schemapb.DataType_Double,
		Nullable: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})

	// the target schema has a new dynamic field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:   200,
		Name:      "new",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})
}

func TestBinlogReader(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

func TestStorageV3Reader_UsesManifestDeltalogPaths(t *testing.T) {
	segmentPath := "backup/insert_log/1/2/3"
	manifestPath := packed.MarshalManifestPath(segmentPath, 7)
	readablePath := segmentPath + "/_delta/2"

	manifestReaderPatch := mockey.Mock(storage.NewManifestRecordReader).
		Return(&storageV3DeltaRecordReader{read: true}, nil).Build()
	defer manifestReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaManifestPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).
		Return([]string{readablePath}, nil).Build()
	defer deltaManifestPatch.UnPatch()

	var readerPaths []string
	deltaReaderPatch := mockey.Mock(storage.NewDeltalogReader).To(
		func(_ context.Context, _ schemapb.DataType, paths []string, _ ...storage.RwOption) (storage.RecordReader, error) {
			readerPaths = append([]string(nil), paths...)
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer deltaReaderPatch.UnPatch()

	r := &reader{
		ctx: context.Background(),
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		storageConfig:  &indexpb.StorageConfig{},
		storageVersion: storage.StorageV3,
		retryAttempts:  1,
	}
	assert.NoError(t, r.initStorageV3Manifest(manifestPath, 0, math.MaxUint64))
	assert.Equal(t, []string{readablePath}, readerPaths)
	assert.Len(t, r.filters, 1)
	r.Close()
}

func TestStorageV3Reader_UsesExactSnapshotManifest(t *testing.T) {
	segmentPath := "snapshot/files/segment/10"
	exactManifest := packed.MarshalManifestPath(segmentPath, 7)
	var openedManifest string

	manifestReaderPatch := mockey.Mock(storage.NewManifestRecordReader).To(
		func(_ context.Context, manifestPath string, _ *schemapb.CollectionSchema, _ ...storage.RwOption) (storage.RecordReader, error) {
			openedManifest = manifestPath
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer manifestReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	r, err := NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		&indexpb.StorageConfig{},
		exactManifest,
		0,
		math.MaxUint64,
		1024,
		"",
		nil,
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, exactManifest, openedManifest)
	r.Close()

	_, err = NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{},
		&indexpb.StorageConfig{},
		packed.MarshalManifestPath(segmentPath, packed.ManifestLatest),
		0,
		math.MaxUint64,
		1024,
		"",
		nil,
		0,
	)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact StorageV3 manifest version")
}

func TestStorageV3Reader_CMEKWithoutTextUsesPackedReaderContext(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	importEzk := "encoded-source-ezk"
	expectedContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 10,
		EncryptionKey:    "encoded-source-key",
	}

	parsePatch := mockey.Mock(hookutil.GetEzIDByImportEzk).To(
		func(got string) (int64, error) {
			assert.Equal(t, importEzk, got)
			return 10, nil
		}).Build()
	defer parsePatch.UnPatch()
	contextPatch := mockey.Mock(hookutil.GetCPluginContextByEzID).To(
		func(got int64) (*indexcgopb.StoragePluginContext, error) {
			assert.Equal(t, int64(10), got)
			return expectedContext, nil
		}).Build()
	defer contextPatch.UnPatch()
	encryptionPatch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer encryptionPatch.UnPatch()
	fieldIDsPatch := mockey.Mock(packed.GetManifestFieldIDs).
		Return(map[int64]struct{}{100: {}}, nil).Build()
	defer fieldIDsPatch.UnPatch()

	var capturedContext *indexcgopb.StoragePluginContext
	innerReaderPatch := mockey.Mock(storage.NewRecordReaderFromManifest).To(
		func(_ string,
			_ *schemapb.CollectionSchema,
			_ int64,
			_ *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			_ ...storage.RwOption,
		) (storage.RecordReader, error) {
			capturedContext = pluginContext
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer innerReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	r, err := NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		&indexpb.StorageConfig{},
		manifestPath,
		0,
		math.MaxUint64,
		1024,
		importEzk,
		nil,
		0,
	)
	require.NoError(t, err)
	require.Same(t, expectedContext, capturedContext)
	r.Close()
}

func TestStorageV3Reader_CMEKWithTextIsUnsupported(t *testing.T) {
	_, err := NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_Text},
		}},
		&indexpb.StorageConfig{},
		packed.MarshalManifestPath("snapshot/files/segment/10", 7),
		0,
		math.MaxUint64,
		1024,
		"encoded-source-ezk",
		nil,
		0,
	)
	assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
	assert.ErrorContains(t, err, "TEXT/LOB")
}

func TestStorageV3Reader_RejectsLegacyPathInput(t *testing.T) {
	r := &reader{storageVersion: storage.StorageV3}
	err := r.init([]string{"backup/insert_log/1/2/3"}, 0, math.MaxUint64)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact manifest from a snapshot source")
}

func TestStorageV3Reader_CollectsManifestReferencedFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	partitionPath := path.Join(root, "backup/insert_log/1/2")
	segmentPath := path.Join(partitionPath, "10")
	manifestPath := packed.MarshalManifestPath(segmentPath, 7)
	segmentDataPath := path.Join(segmentPath, "_data/0.parquet")
	orphanDataPath := path.Join(segmentPath, "_data/orphan.parquet")
	lobWithSizePath := path.Join(partitionPath, "lobs/101/_data/a.vx")
	lobWithoutSizePath := path.Join(partitionPath, "lobs/101/_data/b.vx")
	cm := storage.NewLocalChunkManager()
	assert.NoError(t, cm.Write(ctx, segmentDataPath, []byte("segment-10")))
	assert.NoError(t, cm.Write(ctx, orphanDataPath, []byte("orphan")))
	assert.NoError(t, cm.Write(ctx, lobWithoutSizePath, []byte("lob")))

	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).To(
		func(gotManifest string, _ *indexpb.StorageConfig, columns []string) ([]packed.Fragment, error) {
			assert.Equal(t, manifestPath, gotManifest)
			assert.Nil(t, columns)
			return []packed.Fragment{{FilePath: segmentDataPath}}, nil
		}).Build()
	defer fragmentsPatch.UnPatch()

	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{
		{Path: lobWithSizePath, FileSizeBytes: 64},
		{Path: lobWithoutSizePath},
	}, nil).Build()
	defer lobPatch.UnPatch()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		storageVersion: storage.StorageV3,
		fileSize:       atomic.NewInt64(0),
	}
	assert.NoError(t, r.collectStorageV3Files(manifestPath))
	assert.ElementsMatch(t, []string{
		segmentDataPath,
		lobWithoutSizePath,
	}, r.storageV3Files)
	assert.NotContains(t, r.storageV3Files, orphanDataPath)
	assert.Equal(t, int64(64), r.storageV3LobSize)
	size, err := r.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("segment-10")+len("lob"))+64, size)
}

func TestStorageV3Reader_RejectsInvalidManifestSizeMetadata(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("backup/insert_log/1/2/3", 7)
	tests := []struct {
		name          string
		fragments     []packed.Fragment
		lobFiles      []packed.LobFileInfo
		fragmentErr   error
		lobErr        error
		wantError     string
		dataIntegrity bool
	}{
		{
			name:        "data fragment read failure",
			fragmentErr: errors.New("fragment read failed"),
			wantError:   "failed to read StorageV3 data files from manifest",
		},
		{
			name:          "data fragment without path",
			fragments:     []packed.Fragment{{}},
			wantError:     "data fragment without a path",
			dataIntegrity: true,
		},
		{
			name:      "LOB metadata read failure",
			lobErr:    errors.New("LOB metadata read failed"),
			wantError: "failed to read StorageV3 LOB files from manifest",
		},
		{
			name:          "LOB file without path",
			lobFiles:      []packed.LobFileInfo{{FileSizeBytes: 1}},
			wantError:     "LOB file without a path",
			dataIntegrity: true,
		},
		{
			name:          "LOB file with negative size",
			lobFiles:      []packed.LobFileInfo{{Path: "lobs/1", FileSizeBytes: -1}},
			wantError:     "with negative size",
			dataIntegrity: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
				Return(test.fragments, test.fragmentErr).Build()
			defer fragmentsPatch.UnPatch()
			lobPatch := mockey.Mock(packed.GetManifestLobFiles).
				Return(test.lobFiles, test.lobErr).Build()
			defer lobPatch.UnPatch()

			r := &reader{ctx: context.Background()}
			err := r.collectStorageV3Files(manifestPath)
			assert.ErrorContains(t, err, test.wantError)
			if test.dataIntegrity {
				assert.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
}

func TestStorageV3Reader_ReadsManifestDeletes(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 3}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{5, 12, 18, 25}, nil)
	rawRecord := builder.NewRecord()
	builder.Release()
	record := storage.NewSimpleArrowRecord(rawRecord, map[storage.FieldID]int{
		0:                     0,
		common.TimeStampField: 1,
	})

	readerPatch := mockey.Mock(storage.NewDeltalogReader).To(
		func(_ context.Context, pkType schemapb.DataType, paths []string, _ ...storage.RwOption) (storage.RecordReader, error) {
			assert.Equal(t, schemapb.DataType_Int64, pkType)
			assert.Equal(t, []string{"delta.parquet"}, paths)
			return &storageV3DeltaRecordReader{record: record}, nil
		}).Build()
	defer readerPatch.UnPatch()

	r := &reader{
		ctx: context.Background(),
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
	}
	deletes, err := r.readDeleteV3([]string{"delta.parquet"}, 10, 20)
	assert.NoError(t, err)
	assert.Equal(t, map[any]typeutil.Timestamp{int64(2): 18}, deletes)
}

func TestStorageV3Reader_SourceTimestampInvariant(t *testing.T) {
	paramtable.Init()
	manifest := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, tc := range []struct {
			name       string
			legacy     bool
			commitTs   uint64
			rowTs      int64
			deleteTs   uint64
			start, end uint64
			wantRows   int
			invalid    bool
		}{
			{name: "no_context_keeps_raw_delete_semantics", legacy: true, rowTs: 100, deleteTs: 200, end: math.MaxUint64},
			{name: "zero_commit_is_not_an_upper_bound", rowTs: 400, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "row_before_commit", commitTs: 300, rowTs: 100, deleteTs: 200, end: math.MaxUint64, wantRows: 1},
			{name: "row_equals_commit", commitTs: 300, rowTs: 300, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "delete_equals_commit", commitTs: 300, rowTs: 100, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "delete_after_commit", commitTs: 300, rowTs: 100, deleteTs: 400, end: math.MaxUint64},
			{name: "unsigned_commit_timestamp", commitTs: math.MaxUint64, rowTs: 100, deleteTs: 400, end: math.MaxUint64, wantRows: 1},
			{name: "unsigned_delete_timestamp", commitTs: 300, rowTs: 100, deleteTs: math.MaxUint64, end: math.MaxUint64},
			{name: "range_still_uses_raw_timestamp", commitTs: 300, rowTs: 100, start: 90, end: 110, wantRows: 1},
			{name: "row_after_commit", commitTs: 300, rowTs: 400, end: math.MaxUint64, invalid: true},
			{name: "range_cannot_hide_invalid_row", commitTs: 300, rowTs: 400, end: 200, invalid: true},
			{name: "delete_cannot_hide_invalid_row", commitTs: 300, rowTs: 400, deleteTs: 500, end: math.MaxUint64, invalid: true},
		} {
			t.Run(pkType.String()+"/"+tc.name, func(t *testing.T) {
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true},
				}}
				var pk any = int64(1)
				var arrowPK arrow.DataType = arrow.PrimitiveTypes.Int64
				if pkType == schemapb.DataType_VarChar {
					pk, arrowPK = "key", arrow.BinaryTypes.String
				}
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
					{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
					{Name: "pk", Type: arrowPK},
				}, nil))
				builder.Field(0).(*array.Int64Builder).Append(1)
				builder.Field(1).(*array.Int64Builder).Append(tc.rowTs)
				if pkType == schemapb.DataType_Int64 {
					builder.Field(2).(*array.Int64Builder).Append(pk.(int64))
				} else {
					builder.Field(2).(*array.StringBuilder).Append(pk.(string))
				}
				rawRecord := builder.NewRecord()
				builder.Release()
				owner := &storageV3DeltaRecordReader{record: storage.NewSimpleArrowRecord(rawRecord, map[storage.FieldID]int{
					common.RowIDField: 0, common.TimeStampField: 1, 100: 2,
				})}
				defer owner.Close()
				manifestPatch := mockey.Mock(storage.NewManifestRecordReader).Return(owner, nil).Build()
				defer manifestPatch.UnPatch()
				fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
				defer fragmentsPatch.UnPatch()
				lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
				defer lobPatch.UnPatch()
				deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
				defer deltaPatch.UnPatch()
				var source *internalpb.SnapshotImportSource
				if !tc.legacy {
					source = &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, SourceCommitTimestamp: tc.commitTs}
				}
				r, err := NewStorageV3ManifestReader(context.Background(), nil, schema, &indexpb.StorageConfig{},
					manifest, tc.start, tc.end, 1024, "", source, 1024)
				require.NoError(t, err)
				defer r.Close()
				r.deleteData = make(map[any]typeutil.Timestamp)
				if tc.deleteTs != 0 {
					r.deleteData[pk] = tc.deleteTs
				}
				deleteFilter, err := FilterWithDelete(r)
				require.NoError(t, err)
				r.filters = append(r.filters, deleteFilter)

				data, err := r.Read()
				if tc.invalid {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
					require.Nil(t, data)
					require.Nil(t, r.dr, "invalid source closes the deserializer immediately")
					require.Nil(t, owner.record, "borrowed Arrow record was released")
					_, nextErr := r.Read()
					require.Same(t, err, nextErr, "retry must not skip the invalid source row")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.wantRows, data.GetRowNum())
				if tc.wantRows > 0 {
					require.Equal(t, tc.rowTs, data.Data[common.TimeStampField].GetRow(0), "do not rewrite raw timestamps")
				}
				_, err = r.Read()
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func TestStorageV3Reader_RejectsUnsupportedSourceBeforeIO(t *testing.T) {
	paramtable.Init()
	manifest := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	for _, tc := range []struct {
		name   string
		source *internalpb.SnapshotImportSource
		want   error
	}{
		{"version", &internalpb.SnapshotImportSource{Version: 3, ManifestPath: manifest}, merr.ErrServiceUnimplemented},
		{"manifest_mismatch", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: "other"}, merr.ErrServiceInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
				manifest, 0, math.MaxUint64, 1024, "", tc.source, 1024)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, r)
		})
	}
}

func TestStorageV3Reader_InvalidSourceManifest(t *testing.T) {
	paramtable.Init()
	for _, manifest := range []string{"", "not-json", packed.MarshalManifestPath("snapshot/segment/10", packed.ManifestLatest)} {
		t.Run(manifest, func(t *testing.T) {
			r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
				manifest, 0, math.MaxUint64, 1024, "", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, 1024)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.Nil(t, r)
		})
	}
	manifest := packed.MarshalManifestPath("snapshot/segment/10", 7)
	openErr := merr.WrapErrIoKeyNotFound("missing source object")
	openPatch := mockey.Mock(storage.NewManifestRecordReader).Return(nil, openErr).Build()
	defer openPatch.UnPatch()
	r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
		manifest, 0, math.MaxUint64, 1024, "", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, 1024)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	require.Nil(t, r)
}

func TestStorageV3Reader_ReadFailurePaths(t *testing.T) {
	type timestampValueReader struct {
		storage.DeserializeReader[*storage.Value]
	}
	paramtable.Init()
	for _, name := range []string{"allocate_batch", "deserialize", "allocate_field", "append_field", "filter_batch", "missing_field", "buffer_boundary"} {
		t.Run(name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			r := newReader(context.Background(), nil, schema, nil, storage.StorageV3, 1, "")
			r.snapshotSource = &internalpb.SnapshotImportSource{Version: 1, SourceCommitTimestamp: 200}
			owner := &timestampValueReader{}
			r.dr = owner
			closePatch := mockey.Mock((*timestampValueReader).Close).Return(nil).Build()
			defer closePatch.UnPatch()
			defer r.Close()
			cause := merr.WrapErrIoKeyNotFound("source object")
			read := 0
			rows := 1
			if name == "buffer_boundary" {
				rows = 101
			}
			nextPatch := mockey.Mock((*timestampValueReader).NextValue).To(func(_ *timestampValueReader) (**storage.Value, error) {
				if name == "deserialize" {
					return nil, cause
				}
				if read == rows {
					return nil, io.EOF
				}
				read++
				row := map[int64]any{common.RowIDField: int64(read), common.TimeStampField: int64(100), 100: int64(read)}
				if name == "append_field" {
					row[100] = "not an int64"
				}
				v := &storage.Value{Timestamp: 100, Value: row}
				return &v, nil
			}).Build()
			defer nextPatch.UnPatch()

			data, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
			require.NoError(t, err)
			if name == "missing_field" || name == "allocate_field" {
				delete(data.Data, 100)
			}
			allocations := 0
			batchPatch := mockey.Mock(storage.NewInsertDataWithFunctionOutputField).To(func(_ *schemapb.CollectionSchema) (*storage.InsertData, error) {
				allocations++
				if name == "allocate_batch" || (name == "filter_batch" && allocations == 2) {
					return nil, cause
				}
				return data, nil
			}).Build()
			defer batchPatch.UnPatch()
			if name == "allocate_field" {
				fieldPatch := mockey.Mock(storage.NewFieldData).Return(nil, cause).Build()
				defer fieldPatch.UnPatch()
			}
			if name == "filter_batch" {
				r.filters = []Filter{func(_ map[int64]interface{}) bool { return false }}
			}

			result, err := r.Read()
			switch name {
			case "missing_field":
				require.NoError(t, err)
				require.Equal(t, 1, result.GetRowNum())
			case "buffer_boundary":
				require.NoError(t, err)
				require.Equal(t, 100, result.GetRowNum())
				require.Equal(t, 100, read, "reader must return a bounded batch before EOF")
			case "append_field":
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Nil(t, result)
			default:
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound, "preserve dependency errors instead of returning partial rows")
				require.Nil(t, result)
			}
		})
	}
}

func TestStorageV3Reader_DeleteFilterRequiresPrimaryKey(t *testing.T) {
	f, err := FilterWithDelete(&reader{schema: &schemapb.CollectionSchema{}})
	require.Error(t, err)
	require.Nil(t, f)
}

func TestDeltaLogListing_RetryOnTransientError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds immediately with one field
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: first call returns partial results + transient error, second call succeeds with empty result
	// Empty result triggers early return (len(deltaLogs) == 0) so readDelete is never called.
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			if deltaCallCount == 1 {
				walkFunc(&storage.ChunkObjectInfo{FilePath: deltaPrefix + "partial"})
				return errors.New("net/http: timeout awaiting response headers")
			}
			// Second attempt: empty walk (no delta logs) — triggers early nil return
			return nil
		}).Times(2)

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 2, deltaCallCount, "delta log WalkWithPrefix should have retried on transient error")
}

func TestDeltaLogListing_NonRetryableErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: return non-retryable error
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			return merr.WrapErrIoPermissionDenied(deltaPrefix, errors.New("access denied"))
		}).Once()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, deltaCallCount, "non-retryable error should not retry")
}

func TestMultiReadWithRetry_NonRetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			return nil, merr.WrapErrIoPermissionDenied("test/path", fmt.Errorf("access denied"))
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	_, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.Error(t, err)
	assert.True(t, merr.IsNonRetryableErr(err))
	assert.Equal(t, 1, callCount, "non-retryable error should not be retried")
}

func TestMultiReadWithRetry_RetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			if callCount < 3 {
				return nil, merr.WrapErrIoFailed("test/path", fmt.Errorf("transient error"))
			}
			return [][]byte{[]byte("data")}, nil
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	result, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("data")}, result)
	assert.Equal(t, 3, callCount, "retryable error should be retried until success")
}
