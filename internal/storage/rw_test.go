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

package storage

import (
	"context"
	"io"
	"math"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPackedBinlogRecordSuite(t *testing.T) {
	suite.Run(t, new(PackedBinlogRecordSuite))
}

type PackedBinlogRecordSuite struct {
	suite.Suite

	ctx          context.Context
	mockID       atomic.Int64
	logIDAlloc   allocator.Interface
	mockBinlogIO *mock_util.MockBinlogIO

	collectionID  UniqueID
	partitionID   UniqueID
	segmentID     UniqueID
	schema        *schemapb.CollectionSchema
	maxRowNum     int64
	chunkSize     uint64
	storageConfig *indexpb.StorageConfig
}

func (s *PackedBinlogRecordSuite) SetupTest() {
	ctx := context.Background()
	s.ctx = ctx
	logIDAlloc := allocator.NewLocalAllocator(1, math.MaxInt64)
	s.logIDAlloc = logIDAlloc
	// initcore.InitLocalArrowFileSystem("/tmp")
	s.mockID.Store(time.Now().UnixMilli())
	s.mockBinlogIO = mock_util.NewMockBinlogIO(s.T())
	s.collectionID = UniqueID(0)
	s.partitionID = UniqueID(0)
	s.segmentID = UniqueID(0)
	s.schema = generateTestSchema()
	// s.rootPath = "/tmp"
	// s.bucketName = "a-bucket"
	s.maxRowNum = int64(1000)
	s.chunkSize = uint64(1024)
	s.storageConfig = &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    "/tmp",
		BucketName:  "a-bucket",
	}
}

func (s *PackedBinlogRecordSuite) TestPackedBinlogRecordIntegration() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	rows := 10000
	readBatchSize := 1024
	columnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			Fields:  []int64{0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 101},
		},
		{
			GroupID: 102,
			Columns: []int{13},
			Fields:  []int64{102},
		},
		{
			GroupID: 103,
			Columns: []int{14},
			Fields:  []int64{103},
		},
		{
			GroupID: 104,
			Columns: []int{15},
			Fields:  []int64{104},
		},
		{
			GroupID: 105,
			Columns: []int{16},
			Fields:  []int64{105},
		},
		{
			GroupID: 106,
			Columns: []int{17},
			Fields:  []int64{106},
		},
	}
	wOption := []RwOption{
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return s.mockBinlogIO.Upload(ctx, kvs)
		}),
		WithVersion(StorageV2),
		WithMultiPartUploadSize(0),
		WithBufferSize(1 * 1024 * 1024), // 1MB
		WithColumnGroups(columnGroups),
		WithStorageConfig(s.storageConfig),
	}

	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.NoError(err)

	blobs, err := generateTestData(rows)
	s.NoError(err)

	reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
	s.NoError(err)
	defer reader.Close()

	for i := 1; i <= rows; i++ {
		value, err := reader.NextValue()
		s.NoError(err)
		rec, err := ValueSerializer([]*Value{*value}, s.schema)
		s.NoError(err)
		err = w.Write(rec)
		s.NoError(err)
	}
	err = w.Close()
	s.NoError(err)
	writtenUncompressed := w.GetWrittenUncompressed()
	s.Positive(writtenUncompressed)

	rowNum := w.GetRowNum()
	s.Equal(rowNum, int64(rows))

	fieldBinlogs, statsLog, bm25StatsLog, _, _ := w.GetLogs()
	s.Equal(len(fieldBinlogs), len(columnGroups))
	for _, columnGroup := range fieldBinlogs {
		s.Equal(len(columnGroup.Binlogs), 1)
		s.Equal(columnGroup.Binlogs[0].EntriesNum, int64(rows))
		s.Positive(columnGroup.Binlogs[0].MemorySize)
		s.Positive(columnGroup.Binlogs[0].LogSize, "compressed LogSize should be populated by CloseAndTell")
	}

	s.Equal(len(statsLog.Binlogs), 1)
	s.Equal(statsLog.Binlogs[0].EntriesNum, int64(rows))

	s.Equal(len(bm25StatsLog), 0)

	binlogs := SortFieldBinlogs(fieldBinlogs)
	rOption := []RwOption{
		WithVersion(StorageV2),
		WithStorageConfig(s.storageConfig),
	}
	r, err := NewBinlogRecordReader(s.ctx, binlogs, s.schema, rOption...)
	s.NoError(err)
	defer r.Close()
	for i := 0; i < rows/readBatchSize+1; i++ {
		rec, err := r.Next()
		s.NoError(err)
		if i < rows/readBatchSize {
			s.Equal(rec.Len(), readBatchSize)
		} else {
			s.Equal(rec.Len(), rows%readBatchSize)
		}
	}

	_, err = r.Next()
	s.Equal(err, io.EOF)
	err = r.Close()
	s.NoError(err)

	// Fill contract on the packed (StorageV2) path: schema fields absent from the
	// written files are backfilled by the reader (#52781) — a plain nullable field
	// comes back all-null, a field with a declared default comes back default-filled.
	extSchema := typeutil.Clone(s.schema)
	extSchema.Fields = append(extSchema.Fields,
		&schemapb.FieldSchema{FieldID: 200, Name: "added_nullable", DataType: schemapb.DataType_Int64, Nullable: true},
		&schemapb.FieldSchema{
			FieldID: 201, Name: "added_default", DataType: schemapb.DataType_Int64, Nullable: true,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
		},
	)
	fr, err := NewBinlogRecordReader(s.ctx, binlogs, extSchema, rOption...)
	s.NoError(err)
	defer fr.Close()
	frec, err := fr.Next()
	s.NoError(err)
	s.Positive(frec.Len())
	s.Equal(frec.Len(), frec.Column(200).NullN(), "absent nullable column arrives all-null")
	s.Equal(0, frec.Column(201).NullN(), "absent default-carrying column arrives default-filled (#52781)")
	col201 := frec.Column(201).(*array.Int64)
	for i := 0; i < col201.Len(); i++ {
		s.EqualValues(42, col201.Value(i))
	}
}

// TestManifestReadFillsAbsentDefaultField is the StorageV3/manifest analog: an
// internal manifest read must present the default of a field that has no column
// in the manifest, not NULL (#52771).
func (s *PackedBinlogRecordSuite) TestManifestReadFillsAbsentDefaultField() {
	dir := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, dir)
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	}()
	storageConfig := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, Fields: []int64{0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 101}},
	}
	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum,
		WithVersion(StorageV3), WithColumnGroups(columnGroups), WithStorageConfig(storageConfig),
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error { return nil }))
	s.NoError(err)
	blobs, err := generateTestData(10)
	s.NoError(err)
	deser, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		v, err := deser.NextValue()
		s.NoError(err)
		rec, err := ValueSerializer([]*Value{*v}, s.schema)
		s.NoError(err)
		s.NoError(w.Write(rec))
	}
	deser.Close()
	s.NoError(w.Close())
	_, _, _, manifestPath, _ := w.GetLogs()
	s.NotEmpty(manifestPath)

	const absentFieldID = int64(200)
	readSchema := &schemapb.CollectionSchema{
		Name: s.schema.GetName(),
		Fields: append(append([]*schemapb.FieldSchema{}, s.schema.GetFields()...),
			&schemapb.FieldSchema{
				FieldID: absentFieldID, Name: "added_def", DataType: schemapb.DataType_Int64,
				Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}},
			}),
	}
	r, err := NewManifestRecordReader(s.ctx, manifestPath, readSchema, WithVersion(StorageV3), WithStorageConfig(storageConfig))
	s.NoError(err)
	defer r.Close()
	total := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		s.NoError(err)
		added := rec.Column(absentFieldID).(*array.Int64)
		s.Equal(0, added.NullN(), "absent default field must not be null")
		for j := 0; j < added.Len(); j++ {
			s.Equal(int64(7), added.Value(j))
		}
		s.NotNil(rec.Column(int64(13)))
		total += rec.Len()
	}
	s.Equal(10, total)
}

// writeV2Segment writes a 10-row StorageV2 packed segment and returns its
// FieldBinlogs (which the V2 writer populates with ChildFields).
func (s *PackedBinlogRecordSuite) writeV2Segment(storageConfig *indexpb.StorageConfig, columnGroups []storagecommon.ColumnGroup) []*datapb.FieldBinlog {
	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum,
		WithVersion(StorageV2), WithColumnGroups(columnGroups), WithStorageConfig(storageConfig),
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error { return nil }))
	s.NoError(err)
	blobs, err := generateTestData(10)
	s.NoError(err)
	deser, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		v, err := deser.NextValue()
		s.NoError(err)
		rec, err := ValueSerializer([]*Value{*v}, s.schema)
		s.NoError(err)
		s.NoError(w.Write(rec))
	}
	deser.Close()
	s.NoError(w.Close())
	fieldBinlogs, _, _, _, _ := w.GetLogs()
	return SortFieldBinlogs(fieldBinlogs)
}

func newAbsentDefaultReadSchema(base *schemapb.CollectionSchema, absentFieldID int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: base.GetName(),
		Fields: append(append([]*schemapb.FieldSchema{}, base.GetFields()...),
			&schemapb.FieldSchema{
				FieldID: absentFieldID, Name: "added_def", DataType: schemapb.DataType_Int64,
				Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}},
			}),
	}
}

// TestBinlogReadFillsAbsentDefaultFieldV2 is the StorageV2/binlog analog of the
// manifest test: a packed binlog read must present the default of an added field
// that has no column in the segment, sourcing physical presence from the
// FieldBinlog ChildFields written by the V2 writer.
func (s *PackedBinlogRecordSuite) TestBinlogReadFillsAbsentDefaultFieldV2() {
	dir := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, dir)
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	}()
	storageConfig := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, Fields: []int64{0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 101}},
		{GroupID: 102, Columns: []int{13}, Fields: []int64{102}},
		{GroupID: 103, Columns: []int{14}, Fields: []int64{103}},
		{GroupID: 104, Columns: []int{15}, Fields: []int64{104}},
		{GroupID: 105, Columns: []int{16}, Fields: []int64{105}},
		{GroupID: 106, Columns: []int{17}, Fields: []int64{106}},
	}
	binlogs := s.writeV2Segment(storageConfig, columnGroups)
	s.NotEmpty(binlogs[0].GetChildFields(), "V2 writer must populate ChildFields for presence to be derivable")

	const absentFieldID = int64(200)
	readSchema := newAbsentDefaultReadSchema(s.schema, absentFieldID)
	r, err := NewBinlogRecordReader(s.ctx, binlogs, readSchema, WithVersion(StorageV2), WithStorageConfig(storageConfig))
	s.NoError(err)
	defer r.Close()
	total := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		s.NoError(err)
		added := rec.Column(absentFieldID).(*array.Int64)
		s.Equal(0, added.NullN(), "absent default field must not be null")
		for j := 0; j < added.Len(); j++ {
			s.Equal(int64(7), added.Value(j))
		}
		total += added.Len()
	}
	s.Equal(10, total)
}

// TestBinlogReadNoChildFieldsFallsBackUnfiltered pins the import-restore safety net:
// binlogs without ChildFields (FieldID keyed by column-group ID, the shape import
// reconstructs) are not filterable, so the reader passes the full schema through
// unchanged — no PK strip / no crash — instead of default-filling.
func (s *PackedBinlogRecordSuite) TestBinlogReadNoChildFieldsFallsBackUnfiltered() {
	dir := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, dir)
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	}()
	storageConfig := &indexpb.StorageConfig{RootPath: dir, StorageType: "local"}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, Fields: []int64{0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 101}},
		{GroupID: 102, Columns: []int{13}, Fields: []int64{102}},
		{GroupID: 103, Columns: []int{14}, Fields: []int64{103}},
		{GroupID: 104, Columns: []int{15}, Fields: []int64{104}},
		{GroupID: 105, Columns: []int{16}, Fields: []int64{105}},
		{GroupID: 106, Columns: []int{17}, Fields: []int64{106}},
	}
	binlogs := s.writeV2Segment(storageConfig, columnGroups)
	for _, fb := range binlogs { // simulate import-reconstructed binlogs: drop ChildFields
		fb.ChildFields = nil
	}

	const absentFieldID = int64(200)
	readSchema := newAbsentDefaultReadSchema(s.schema, absentFieldID)
	r, err := NewBinlogRecordReader(s.ctx, binlogs, readSchema, WithVersion(StorageV2), WithStorageConfig(storageConfig))
	s.NoError(err) // must not strip the PK / fail — regression guard for the import break
	defer r.Close()
	total := 0
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		s.NoError(err)
		added := rec.Column(absentFieldID).(*array.Int64)
		s.Equal(added.Len(), added.NullN(), "no-ChildFields fallback must not default-fill")
		s.NotNil(rec.Column(int64(13))) // present column still reads back
		total += rec.Len()
	}
	s.Equal(10, total)
}

func (s *PackedBinlogRecordSuite) TestGenerateBM25Stats() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.schema = genCollectionSchemaWithBM25()
	columnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0, 1, 2},
		},
		{
			GroupID: 101,
			Columns: []int{3},
		},
		{
			GroupID: 102,
			Columns: []int{4},
		},
	}
	wOption := []RwOption{
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return s.mockBinlogIO.Upload(ctx, kvs)
		}),
		WithVersion(StorageV2),
		WithMultiPartUploadSize(0),
		WithBufferSize(10 * 1024 * 1024), // 10MB
		WithColumnGroups(columnGroups),
		WithStorageConfig(s.storageConfig),
	}

	v := &Value{
		PK:        NewVarCharPrimaryKey("0"),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday())),
		Value:     genRowWithBM25(0),
	}
	rec, err := ValueSerializer([]*Value{v}, s.schema)
	s.NoError(err)

	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.NoError(err)
	err = w.Write(rec)
	s.NoError(err)
	err = w.Close()
	s.NoError(err)
	fieldBinlogs, statsLog, bm25StatsLog, _, _ := w.GetLogs()
	s.Equal(len(fieldBinlogs), len(columnGroups))

	s.Equal(statsLog.Binlogs[0].EntriesNum, int64(1))
	s.Positive(statsLog.Binlogs[0].MemorySize)

	s.Equal(len(bm25StatsLog), 1)
	s.Equal(bm25StatsLog[102].Binlogs[0].EntriesNum, int64(1))
	s.Positive(bm25StatsLog[102].Binlogs[0].MemorySize)
}

func (s *PackedBinlogRecordSuite) TestUnsuportedStorageVersion() {
	wOption := []RwOption{
		WithVersion(-1),
		WithStorageConfig(s.storageConfig),
	}
	_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.Error(err)

	rOption := []RwOption{
		WithVersion(-1),
	}
	_, err = NewBinlogRecordReader(s.ctx, []*datapb.FieldBinlog{{}}, s.schema, rOption...)
	s.Error(err)
}

func (s *PackedBinlogRecordSuite) TestStorageV1RejectsNullableArrayOfVectorWriter() {
	s.schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
		{
			Name:     "struct_array",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     200,
					Name:        "embeddings",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					Nullable:    true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
						{Key: common.MaxCapacityKey, Value: "8"},
					},
				},
			},
		},
	}

	_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum,
		WithVersion(StorageV1),
		WithUploader(func(context.Context, map[string][]byte) error { return nil }),
		WithStorageConfig(s.storageConfig),
	)
	s.Error(err)
	s.ErrorIs(err, merr.ErrStorage)
	s.Contains(err.Error(), "nullable ArrayOfVector is not supported in V1 storage format")
}

func (s *PackedBinlogRecordSuite) TestStorageV1RejectsElementNullableArrayWriter() {
	testCases := []struct {
		name      string
		mutate    func(*schemapb.CollectionSchema)
		errString string
	}{
		{
			name: "top level array",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID:         200,
					Name:            "array",
					DataType:        schemapb.DataType_Array,
					ElementType:     schemapb.DataType_Int64,
					ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxCapacityKey, Value: "8"},
					},
				})
			},
			errString: "element nullable Array is not supported in V1 storage format",
		},
		{
			name: "top level array of vector",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID:         201,
					Name:            "vector_array",
					DataType:        schemapb.DataType_ArrayOfVector,
					ElementType:     schemapb.DataType_FloatVector,
					ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
						{Key: common.MaxCapacityKey, Value: "8"},
					},
				})
			},
			errString: "element nullable ArrayOfVector is not supported in V1 storage format",
		},
		{
			name: "struct array",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
					{
						Name:     "struct_array",
						Nullable: true,
						Fields: []*schemapb.FieldSchema{
							{
								FieldID:         202,
								Name:            "array",
								DataType:        schemapb.DataType_Array,
								ElementType:     schemapb.DataType_Int64,
								ElementNullable: true,
								TypeParams: []*commonpb.KeyValuePair{
									{Key: common.MaxCapacityKey, Value: "8"},
								},
							},
						},
					},
				}
			},
			errString: "element nullable Array is not supported in V1 storage format",
		},
		{
			name: "struct array of vector",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
					{
						Name:     "struct_array",
						Nullable: true,
						Fields: []*schemapb.FieldSchema{
							{
								FieldID:         203,
								Name:            "embeddings",
								DataType:        schemapb.DataType_ArrayOfVector,
								ElementType:     schemapb.DataType_FloatVector,
								ElementNullable: true,
								TypeParams: []*commonpb.KeyValuePair{
									{Key: common.DimKey, Value: "4"},
									{Key: common.MaxCapacityKey, Value: "8"},
								},
							},
						},
					},
				}
			},
			errString: "element nullable ArrayOfVector is not supported in V1 storage format",
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.Run(tc.name, func() {
			schema := generateTestSchema()
			tc.mutate(schema)

			_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, schema, s.logIDAlloc, s.chunkSize, s.maxRowNum,
				WithVersion(StorageV1),
				WithUploader(func(context.Context, map[string][]byte) error { return nil }),
				WithStorageConfig(s.storageConfig),
			)
			s.Error(err)
			s.ErrorIs(err, merr.ErrStorage)
			s.Contains(err.Error(), tc.errString)
		})
	}
}

func (s *PackedBinlogRecordSuite) TestStorageV1RejectsNestedArrayWriter() {
	s.schema.Fields = append(s.schema.Fields, &schemapb.FieldSchema{
		FieldID:     200,
		Name:        "nested_array",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_ArrayElement{
				ArrayElement: &schemapb.TypeSchema{
					Kind: &schemapb.TypeSchema_ArrayElement{
						ArrayElement: &schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64},
						},
					},
				},
			},
		},
	})

	_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum,
		WithVersion(StorageV1),
		WithUploader(func(context.Context, map[string][]byte) error { return nil }),
		WithStorageConfig(s.storageConfig),
	)
	s.Error(err)
	s.Contains(err.Error(), "nested Array is not supported in V1 storage format")
}

func (s *PackedBinlogRecordSuite) TestNoPrimaryKeyError() {
	s.schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 13, Name: "field12", DataType: schemapb.DataType_JSON},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0},
		},
	}
	wOption := []RwOption{
		WithVersion(StorageV2),
		WithColumnGroups(columnGroups),
		WithStorageConfig(s.storageConfig),
	}
	_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.Error(err)
}

func (s *PackedBinlogRecordSuite) TestConvertArrowSchemaError() {
	s.schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 14, Name: "field13", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{}},
	}}
	columnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0},
		},
	}
	wOption := []RwOption{
		WithVersion(StorageV2),
		WithColumnGroups(columnGroups),
		WithStorageConfig(s.storageConfig),
	}
	_, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.Error(err)
}

func (s *PackedBinlogRecordSuite) TestEmptyBinlog() {
	rOption := []RwOption{
		WithVersion(StorageV2),
		WithStorageConfig(s.storageConfig),
	}
	_, err := NewBinlogRecordReader(s.ctx, []*datapb.FieldBinlog{}, s.schema, rOption...)
	s.Error(err)
}

func (s *PackedBinlogRecordSuite) TestAllocIDExhausedError() {
	columnGroups := []storagecommon.ColumnGroup{
		{
			GroupID: 0,
			Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		},
	}
	wOption := []RwOption{
		WithVersion(StorageV2),
		WithColumnGroups(columnGroups),
		WithStorageConfig(s.storageConfig),
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			return nil
		}),
	}
	logIDAlloc := allocator.NewLocalAllocator(1, 1)
	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	s.NoError(err)

	size := 10
	blobs, err := generateTestData(size)
	s.NoError(err)

	reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
	s.NoError(err)
	defer reader.Close()

	for i := 0; i < size; i++ {
		value, err := reader.NextValue()
		s.NoError(err)

		rec, err := ValueSerializer([]*Value{*value}, s.schema)
		s.NoError(err)
		err = w.Write(rec)
		s.Error(err)
	}
}

// TestV3StatsWrittenUnderBasePath verifies the regression fix: for V3
// (manifest-based) storage, bloom filter stats must be written to
// basePath/_stats/bloom_filter.{fieldID}/{id}, NOT to stats_log/.
// Before the fix, writeStats() was called, placing files at
// {rootPath}/stats_log/... which caused a mangled path on read-back.
func (s *PackedBinlogRecordSuite) TestV3StatsWrittenUnderBasePath() {
	dir := s.T().TempDir()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, dir)
	defer func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	}()

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}
	columnGroups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, Fields: []int64{0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 101}},
	}
	wOption := []RwOption{
		WithVersion(StorageV3),
		WithColumnGroups(columnGroups),
		WithStorageConfig(storageConfig),
		WithUploader(func(ctx context.Context, kvs map[string][]byte) error { return nil }),
	}

	w, err := NewBinlogRecordWriter(s.ctx, s.collectionID, s.partitionID, s.segmentID, s.schema, s.logIDAlloc, s.chunkSize, s.maxRowNum, wOption...)
	require.NoError(s.T(), err)

	blobs, err := generateTestData(10)
	require.NoError(s.T(), err)
	reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
	require.NoError(s.T(), err)
	defer reader.Close()
	for i := 0; i < 10; i++ {
		v, err := reader.NextValue()
		require.NoError(s.T(), err)
		rec, err := ValueSerializer([]*Value{*v}, s.schema)
		require.NoError(s.T(), err)
		require.NoError(s.T(), w.Write(rec))
	}
	require.NoError(s.T(), w.Close())

	_, statsLog, _, manifestPath, _ := w.GetLogs()

	// For V3: stats are in the manifest, not in a separate FieldBinlog.
	assert.Nil(s.T(), statsLog, "V3 statsLog must be nil; stats are stored in the manifest")
	require.NotEmpty(s.T(), manifestPath, "V3 manifest path must be non-empty")

	// The manifest must contain bloom filter stats with paths under basePath/_stats/.
	stats, err := packed.GetManifestStats(manifestPath, storageConfig)
	require.NoError(s.T(), err)

	pkField, err := typeutil.GetPrimaryFieldSchema(s.schema)
	require.NoError(s.T(), err)
	bfKey := "bloom_filter." + strconv.FormatInt(pkField.GetFieldID(), 10)
	bfStat, ok := stats[bfKey]
	require.True(s.T(), ok, "manifest must contain bloom filter stats under key %q", bfKey)
	require.NotEmpty(s.T(), bfStat.Paths)

	basePath := path.Join(dir, common.SegmentInsertLogPath,
		metautil.JoinIDPath(s.collectionID, s.partitionID, s.segmentID))
	for _, p := range bfStat.Paths {
		assert.True(s.T(), strings.HasPrefix(p, basePath+"/_stats/"),
			"bloom filter stat path %q must be under basePath/_stats/, got path outside basePath", p)
		assert.NotContains(s.T(), p, "stats_log",
			"bloom filter stat path must not use legacy stats_log/ layout")
	}
}

func genRowWithBM25(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday())
	return map[int64]interface{}{
		common.RowIDField:     magic,
		common.TimeStampField: int64(ts),
		100:                   strconv.FormatInt(magic, 10),
		101:                   "varchar",
		102:                   typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1}),
	}
}

func genCollectionSchemaWithBM25() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "Timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_VarChar,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "sparse",
				DataType: schemapb.DataType_SparseFloatVector,
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "BM25",
			Id:               100,
			Type:             schemapb.FunctionType_BM25,
			InputFieldNames:  []string{"text"},
			InputFieldIds:    []int64{101},
			OutputFieldNames: []string{"sparse"},
			OutputFieldIds:   []int64{102},
		}},
	}
}

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}

func Test_makeBlobsReader(t *testing.T) {
	ctx := context.Background()
	downloader := func(ctx context.Context, paths []string) ([][]byte, error) {
		return lo.Map(paths, func(item string, index int) []byte {
			return []byte{}
		}), nil
	}

	tests := []struct {
		name    string
		binlogs []*datapb.FieldBinlog
		want    [][]*Blob
		wantErr bool
	}{
		{
			name: "test full",
			binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/100/1"},
					},
				},
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/101/2"},
					},
				},
				{
					FieldID: 102,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/102/3"},
					},
				},
			},
			want: [][]*Blob{
				{
					{
						Key:   "x/1/1/1/100/1",
						Value: []byte{},
					},
					{
						Key:   "x/1/1/1/101/2",
						Value: []byte{},
					},
					{
						Key:   "x/1/1/1/102/3",
						Value: []byte{},
					},
				},
			},
			wantErr: false,
		},

		{
			name: "test added field",
			binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/100/1"},
						{LogPath: "x/1/1/1/100/3"},
					},
				},
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/101/2"},
						{LogPath: "x/1/1/1/101/4"},
					},
				},
				{
					FieldID: 102,
					Binlogs: []*datapb.Binlog{
						{LogPath: "x/1/1/1/102/5"},
					},
				},
			},
			want: [][]*Blob{
				{
					{
						Key:   "x/1/1/1/100/1",
						Value: []byte{},
					},
					{
						Key:   "x/1/1/1/101/2",
						Value: []byte{},
					},
				},
				{
					{
						Key:   "x/1/1/1/100/3",
						Value: []byte{},
					},
					{
						Key:   "x/1/1/1/101/4",
						Value: []byte{},
					},
					{
						Key:   "x/1/1/1/102/5",
						Value: []byte{},
					},
				},
			},
			wantErr: false,
		},

		// {
		// 	name: "test error",
		// 	binlogs: []*datapb.FieldBinlog{
		// 		{
		// 			FieldID: 100,
		// 			Binlogs: []*datapb.Binlog{
		// 				{LogPath: "x/1/1/1/100/1"},
		// 				{LogPath: "x/1/1/1/100/3"},
		// 			},
		// 		},
		// 		{
		// 			FieldID: 101,
		// 			Binlogs: []*datapb.Binlog{
		// 				{LogPath: "x/1/1/1/101/2"},
		// 				{LogPath: "x/1/1/1/101/4"},
		// 			},
		// 		},
		// 		{
		// 			FieldID: 102,
		// 			Binlogs: []*datapb.Binlog{
		// 				{LogPath: "x/1/1/1/102/5"},
		// 				{LogPath: "x/1/1/1/102/6"},
		// 			},
		// 		},
		// 	},
		// 	want:    nil,
		// 	wantErr: true,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := makeBlobsReader(ctx, tt.binlogs, downloader)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("makeBlobsReader() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			got := make([][]*Blob, 0)
			for {
				bs, err := reader()
				if err == io.EOF {
					break
				}
				if err != nil {
					assert.Fail(t, err.Error())
				}
				got = append(got, bs)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRwOptionValidate(t *testing.T) {
	testCases := []struct {
		tag         string
		input       *rwOptions
		expectError bool
	}{
		{
			tag: "normal_case",
			input: &rwOptions{
				version:       StorageV1,
				storageConfig: &indexpb.StorageConfig{},
				op:            OpRead,
				downloader:    func(ctx context.Context, paths []string) ([][]byte, error) { return nil, nil },
			},
			expectError: false,
		},
		{
			tag: "normal_case_v2",
			input: &rwOptions{
				version:       StorageV2,
				storageConfig: &indexpb.StorageConfig{},
				op:            OpRead,
			},
			expectError: false,
		},
		{
			tag: "bad_version",
			input: &rwOptions{
				version:       -1,
				storageConfig: &indexpb.StorageConfig{},
				downloader:    func(ctx context.Context, paths []string) ([][]byte, error) { return nil, nil },
				op:            OpRead,
			},
			expectError: true,
		},
		{
			tag: "missing_config",
			input: &rwOptions{
				version:       StorageV2,
				storageConfig: nil,
				op:            OpRead,
			},
			expectError: true,
		},
		{
			tag: "v1eader_missing_downloader",
			input: &rwOptions{
				version:       StorageV1,
				storageConfig: &indexpb.StorageConfig{},
				op:            OpRead,
			},
			expectError: true,
		},
		{
			tag: "writer_missing_uploader",
			input: &rwOptions{
				version:       StorageV2,
				storageConfig: &indexpb.StorageConfig{},
				op:            OpWrite,
			},
			expectError: false, // V2 uses storageConfig, uploader not required
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			err := tc.input.validate()
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilterSchemaToPresentFieldsStructAllOrNothing(t *testing.T) {
	// One ordinary field plus a struct array whose two children are the physical
	// (first-level) columns of the struct. A struct array is added/dropped whole,
	// so its children are physically all-or-nothing.
	newSchema := func() *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{FieldID: 200, Name: "st", Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "st[a]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
					{FieldID: 202, Name: "st[b]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
				}},
			},
		}
	}

	t.Run("struct fully present is kept whole", func(t *testing.T) {
		out, err := filterSchemaToPresentFields(newSchema(), map[FieldID]struct{}{100: {}, 201: {}, 202: {}})
		require.NoError(t, err)
		require.Len(t, out.GetStructArrayFields(), 1)
		require.Len(t, out.GetStructArrayFields()[0].GetFields(), 2)
	})

	t.Run("struct fully absent is dropped whole", func(t *testing.T) {
		out, err := filterSchemaToPresentFields(newSchema(), map[FieldID]struct{}{100: {}})
		require.NoError(t, err)
		require.Empty(t, out.GetStructArrayFields())
		require.Len(t, out.GetFields(), 1)
	})

	t.Run("struct partially present is a data-integrity error", func(t *testing.T) {
		_, err := filterSchemaToPresentFields(newSchema(), map[FieldID]struct{}{100: {}, 201: {}})
		require.Error(t, err)
	})
}

func TestFilterSchemaToPresentFieldsDropsAbsentTopLevelAndKeepsAttrs(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:               "coll",
		EnableDynamicField: true,
		Functions:          []*schemapb.FunctionSchema{{Name: "fn", Id: 7}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "absent", DataType: schemapb.DataType_Int64, Nullable: true},
		},
	}
	out, err := filterSchemaToPresentFields(schema, map[FieldID]struct{}{100: {}})
	require.NoError(t, err)
	require.Len(t, out.GetFields(), 1) // absent top-level 101 dropped
	require.Equal(t, int64(100), out.GetFields()[0].GetFieldID())
	// non-field schema attributes survive the proto.Clone-based filter
	require.Equal(t, "coll", out.GetName())
	require.True(t, out.GetEnableDynamicField())
	require.Len(t, out.GetFunctions(), 1)
}

func TestBinlogFieldIDSet(t *testing.T) {
	// flush/compaction binlogs carry ChildFields (the group's member field IDs) -> reliable.
	withChildren := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{0, 1, 100}},
		{FieldID: 101, ChildFields: []int64{101}},
	}
	present, reliable := binlogFieldIDSet(withChildren)
	require.True(t, reliable)
	require.Equal(t, map[FieldID]struct{}{0: {}, 1: {}, 100: {}, 101: {}}, present)

	// import-reconstructed binlogs key FieldID by column-group ID with no ChildFields
	// -> presence is not derivable -> unreliable (caller must read unfiltered).
	noChildren := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{0, 1, 100}},
		{FieldID: 101}, // no ChildFields
	}
	_, reliable = binlogFieldIDSet(noChildren)
	require.False(t, reliable)
}
