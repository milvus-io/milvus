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

package compaction

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCommonSuite(t *testing.T) {
	suite.Run(t, new(CommonSuite))
}

type CommonSuite struct {
	suite.Suite
}

func (s *CommonSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CommonSuite) TestComposeDeleteFromDeltalogs() {
	ctx := context.Background()

	tests := []struct {
		name           string
		pkType         schemapb.DataType
		setupDeltalogs func() (map[string][]byte, []*datapb.FieldBinlog)
		expectedCount  int
		verifyResults  func(pk2Ts map[any]typeutil.Timestamp)
	}{
		{
			name:   "Int64 PK - single deltalog",
			pkType: schemapb.DataType_Int64,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				blob := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3, 4, 5}, []int64{1000, 1001, 1002, 1003, 1004})
				deltalogs := []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog.bin"}},
					},
				}
				return map[string][]byte{"/test/deltalog.bin": blob.Value}, deltalogs
			},
			expectedCount: 5,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {
				s.Equal(typeutil.Timestamp(1000), pk2Ts[int64(1)])
				s.Equal(typeutil.Timestamp(1001), pk2Ts[int64(2)])
				s.Equal(typeutil.Timestamp(1002), pk2Ts[int64(3)])
				s.Equal(typeutil.Timestamp(1003), pk2Ts[int64(4)])
				s.Equal(typeutil.Timestamp(1004), pk2Ts[int64(5)])
			},
		},
		{
			name:   "VarChar PK - single deltalog",
			pkType: schemapb.DataType_VarChar,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				blob := s.createTestDeltaLog(schemapb.DataType_VarChar, []string{"pk_1", "pk_2", "pk_3"}, []int64{2000, 2001, 2002})
				deltalogs := []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog.bin"}},
					},
				}
				return map[string][]byte{"/test/deltalog.bin": blob.Value}, deltalogs
			},
			expectedCount: 3,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {
				s.Equal(typeutil.Timestamp(2000), pk2Ts["pk_1"])
				s.Equal(typeutil.Timestamp(2001), pk2Ts["pk_2"])
				s.Equal(typeutil.Timestamp(2002), pk2Ts["pk_3"])
			},
		},
		{
			name:   "Multiple deltalogs without duplicates",
			pkType: schemapb.DataType_Int64,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				blob1 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3}, []int64{1000, 1001, 1002})
				blob2 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{4, 5, 6}, []int64{2000, 2001, 2002})
				deltalogs := []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog1.bin"}},
					},
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog2.bin"}},
					},
				}
				return map[string][]byte{
					"/test/deltalog1.bin": blob1.Value,
					"/test/deltalog2.bin": blob2.Value,
				}, deltalogs
			},
			expectedCount: 6,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {
				s.Equal(typeutil.Timestamp(1000), pk2Ts[int64(1)])
				s.Equal(typeutil.Timestamp(2000), pk2Ts[int64(4)])
			},
		},
		{
			name:   "Duplicate PKs - keep newer timestamps",
			pkType: schemapb.DataType_Int64,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				blob1 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3}, []int64{1000, 1001, 1002})
				blob2 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3}, []int64{2000, 2001, 2002})
				deltalogs := []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog1.bin"}},
					},
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog2.bin"}},
					},
				}
				return map[string][]byte{
					"/test/deltalog1.bin": blob1.Value,
					"/test/deltalog2.bin": blob2.Value,
				}, deltalogs
			},
			expectedCount: 3,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {
				s.Equal(typeutil.Timestamp(2000), pk2Ts[int64(1)])
				s.Equal(typeutil.Timestamp(2001), pk2Ts[int64(2)])
				s.Equal(typeutil.Timestamp(2002), pk2Ts[int64(3)])
			},
		},
		{
			name:   "Duplicate PKs - skip older timestamps",
			pkType: schemapb.DataType_Int64,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				blob1 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3}, []int64{2000, 2001, 2002})
				blob2 := s.createTestDeltaLog(schemapb.DataType_Int64, []int64{1, 2, 3}, []int64{1000, 1001, 1002})
				deltalogs := []*datapb.FieldBinlog{
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog1.bin"}},
					},
					{
						FieldID: 100,
						Binlogs: []*datapb.Binlog{{LogPath: "/test/deltalog2.bin"}},
					},
				}
				return map[string][]byte{
					"/test/deltalog1.bin": blob1.Value,
					"/test/deltalog2.bin": blob2.Value,
				}, deltalogs
			},
			expectedCount: 3,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {
				s.Equal(typeutil.Timestamp(2000), pk2Ts[int64(1)])
				s.Equal(typeutil.Timestamp(2001), pk2Ts[int64(2)])
				s.Equal(typeutil.Timestamp(2002), pk2Ts[int64(3)])
			},
		},
		{
			name:   "Empty deltalogs",
			pkType: schemapb.DataType_Int64,
			setupDeltalogs: func() (map[string][]byte, []*datapb.FieldBinlog) {
				return map[string][]byte{}, []*datapb.FieldBinlog{}
			},
			expectedCount: 0,
			verifyResults: func(pk2Ts map[any]typeutil.Timestamp) {},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			blobs, deltalogs := tt.setupDeltalogs()

			var options []storage.RwOption
			if len(blobs) > 0 {
				options = []storage.RwOption{
					storage.WithVersion(storage.StorageV1),
					storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
						result := make([][]byte, len(paths))
						for i, path := range paths {
							result[i] = blobs[path]
						}
						return result, nil
					}),
				}
			}

			pk2Ts, err := ComposeDeleteFromDeltalogsV1(ctx, tt.pkType, deltalogs, options...)
			s.NoError(err)
			s.NotNil(pk2Ts)
			s.Equal(tt.expectedCount, len(pk2Ts))

			if tt.verifyResults != nil {
				tt.verifyResults(pk2Ts)
			}
		})
	}
}

// Helper functions to create test deltalog data

// createTestDeltaLog creates a deltalog blob with the given PKs and timestamps.
// For Int64 PKs, pass []int64; for VarChar PKs, pass []string.
func (s *CommonSuite) createTestDeltaLog(pkType schemapb.DataType, pks any, tss []int64) *storage.Blob {
	var record storage.Record
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := pks.([]int64)
		require.Equal(s.T(), len(int64Pks), len(tss), "pks and tss must have same length")
		record = s.createTestRecord(pkType, int64Pks, nil, tss)
	case schemapb.DataType_VarChar:
		stringPks := pks.([]string)
		require.Equal(s.T(), len(stringPks), len(tss), "pks and tss must have same length")
		record = s.createTestRecord(pkType, nil, stringPks, tss)
	default:
		s.FailNow("unsupported pk type")
		return nil
	}
	defer record.Release()

	blob := &storage.Blob{}
	path := "/test/deltalog.bin"

	writer, err := storage.NewLegacyDeltalogWriter(1, 1, 1, 0, pkType,
		func(ctx context.Context, kvs map[string][]byte) error {
			blob.Value = kvs[path]
			blob.Key = path
			return nil
		}, path)
	require.NoError(s.T(), err)

	err = writer.Write(record)
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	return blob
}

func (s *CommonSuite) createTestRecord(pkType schemapb.DataType, int64Pks []int64, stringPks []string, tss []int64) storage.Record {
	allocator := memory.DefaultAllocator

	var pkArray arrow.Array
	var numRows int

	switch pkType {
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(allocator)
		defer builder.Release()
		for _, pk := range int64Pks {
			builder.Append(pk)
		}
		pkArray = builder.NewArray()
		numRows = len(int64Pks)
	case schemapb.DataType_VarChar:
		builder := array.NewStringBuilder(allocator)
		defer builder.Release()
		for _, pk := range stringPks {
			builder.Append(pk)
		}
		pkArray = builder.NewArray()
		numRows = len(stringPks)
	default:
		s.FailNow("unsupported pk type")
	}

	require.Equal(s.T(), numRows, len(tss), "number of pks and tss must match")

	// Create timestamp array
	tsBuilder := array.NewInt64Builder(allocator)
	defer tsBuilder.Release()
	for _, ts := range tss {
		tsBuilder.Append(ts)
	}
	tsArray := tsBuilder.NewArray()

	// Create arrow schema
	var pkFieldType arrow.DataType
	if pkType == schemapb.DataType_Int64 {
		pkFieldType = arrow.PrimitiveTypes.Int64
	} else {
		pkFieldType = arrow.BinaryTypes.String
	}

	pkArrowField := arrow.Field{Name: "pk", Type: pkFieldType, Nullable: false}
	tsField := arrow.Field{Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false}

	schema := arrow.NewSchema([]arrow.Field{pkArrowField, tsField}, nil)
	record := array.NewRecord(schema, []arrow.Array{pkArray, tsArray}, int64(numRows))

	field2Col := map[int64]int{
		0: 0, // pk column
		1: 1, // ts column
	}

	return storage.NewSimpleArrowRecord(record, field2Col)
}

// writeV2DeltaLog writes a V2 deltalog parquet file at the given path.
// path should follow production pattern: filepath.Join(rootPath, "delta_log/collID/partID/segID/logID")
func (s *CommonSuite) writeV2DeltaLog(
	ctx context.Context,
	pkType schemapb.DataType,
	pks any,
	tss []int64,
	path string,
	storageConfig *indexpb.StorageConfig,
) {
	t := s.T()

	var record storage.Record
	switch pkType {
	case schemapb.DataType_Int64:
		int64Pks := pks.([]int64)
		record = s.createTestRecord(pkType, int64Pks, nil, tss)
	case schemapb.DataType_VarChar:
		stringPks := pks.([]string)
		record = s.createTestRecord(pkType, nil, stringPks, tss)
	default:
		s.FailNow("unsupported pk type")
	}
	defer record.Release()

	writer, err := storage.NewDeltalogWriter(ctx, 1, 2, 3, 101, pkType, path,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(storageConfig))
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// createBaseManifest creates a base manifest via FFIPackedWriter for V2 deltalog tests.
// basePath should follow production pattern: filepath.Join(rootPath, "insert_log/collID/partID/segID")
func (s *CommonSuite) createBaseManifest(basePath string, storageConfig *indexpb.StorageConfig) string {
	t := s.T()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "pk",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"100"}),
		},
		{
			Name:     "vector",
			Type:     &arrow.FixedSizeBinaryType{ByteWidth: 16},
			Nullable: false,
			Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{"101"}),
		},
	}, nil)

	columnGroups := []storagecommon.ColumnGroup{
		{Columns: []int{0, 1}, GroupID: storagecommon.DefaultShortColumnGroupID},
	}

	pw, err := packed.NewFFIPackedWriter(basePath, 0, arrowSchema, columnGroups, storageConfig, nil)
	require.NoError(t, err)

	// Write minimal data to create a valid manifest
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	vectorBytes := make([]byte, 16)
	b.Field(1).(*array.FixedSizeBinaryBuilder).Append(vectorBytes)
	rec := b.NewRecord()
	defer rec.Release()

	err = pw.WriteRecordBatch(rec)
	require.NoError(t, err)

	manifestPath, err := pw.Close()
	require.NoError(t, err)

	return manifestPath
}

func (s *CommonSuite) TestComposeDeleteFromDeltalogsV2() {
	ctx := context.Background()

	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	dir := s.T().TempDir()
	pt.Save(pt.LocalStorageCfg.Path.Key, dir)
	s.T().Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})

	storageConfig := &indexpb.StorageConfig{
		RootPath:    dir,
		StorageType: "local",
	}

	// Options that match production callers: downloader (for V1 validate) + storageConfig (for V2 ops)
	options := []storage.RwOption{
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return nil, nil
		}),
		storage.WithStorageConfig(storageConfig),
	}

	s.Run("V2 manifest - Int64 PK", func() {
		// Use basePath matching production pattern: path.Join(rootPath, "insert_log", collID, partID, segID)
		basePath := filepath.Join(dir, "insert_log/1/2/3_int64")
		manifestPath := s.createBaseManifest(basePath, storageConfig)

		// deltaPath matching production: path.Join(rootPath, "delta_log", collID, partID, segID, logID)
		deltaPath := filepath.Join(dir, "delta_log/1/2/3/101")
		s.writeV2DeltaLog(ctx, schemapb.DataType_Int64, []int64{10, 20, 30}, []int64{1000, 1001, 1002}, deltaPath, storageConfig)

		newManifest, err := packed.AddDeltaLogsToManifest(manifestPath, storageConfig, []packed.DeltaLogEntry{
			{Path: deltaPath, NumEntries: 3},
		})
		s.Require().NoError(err)

		segment := &datapb.CompactionSegmentBinlogs{Manifest: newManifest}
		pk2Ts, err := ComposeDeleteFromDeltalogs(ctx, schemapb.DataType_Int64, segment, options...)
		s.NoError(err)
		s.Equal(3, len(pk2Ts))
		s.Equal(typeutil.Timestamp(1000), pk2Ts[int64(10)])
		s.Equal(typeutil.Timestamp(1001), pk2Ts[int64(20)])
		s.Equal(typeutil.Timestamp(1002), pk2Ts[int64(30)])
	})

	s.Run("V2 manifest - VarChar PK", func() {
		basePath := filepath.Join(dir, "insert_log/1/2/3_varchar")
		manifestPath := s.createBaseManifest(basePath, storageConfig)

		deltaPath := filepath.Join(dir, "delta_log/1/2/3/102")
		s.writeV2DeltaLog(ctx, schemapb.DataType_VarChar, []string{"pk_a", "pk_b", "pk_c"}, []int64{2000, 2001, 2002}, deltaPath, storageConfig)

		newManifest, err := packed.AddDeltaLogsToManifest(manifestPath, storageConfig, []packed.DeltaLogEntry{
			{Path: deltaPath, NumEntries: 3},
		})
		s.Require().NoError(err)

		segment := &datapb.CompactionSegmentBinlogs{Manifest: newManifest}
		pk2Ts, err := ComposeDeleteFromDeltalogs(ctx, schemapb.DataType_VarChar, segment, options...)
		s.NoError(err)
		s.Equal(3, len(pk2Ts))
		s.Equal(typeutil.Timestamp(2000), pk2Ts["pk_a"])
		s.Equal(typeutil.Timestamp(2001), pk2Ts["pk_b"])
		s.Equal(typeutil.Timestamp(2002), pk2Ts["pk_c"])
	})

	s.Run("V2 manifest - multiple deltalogs", func() {
		basePath := filepath.Join(dir, "insert_log/1/2/3_multi")
		manifestPath := s.createBaseManifest(basePath, storageConfig)

		deltaPath1 := filepath.Join(dir, "delta_log/1/2/3/201")
		deltaPath2 := filepath.Join(dir, "delta_log/1/2/3/202")
		s.writeV2DeltaLog(ctx, schemapb.DataType_Int64, []int64{100, 200}, []int64{3000, 3001}, deltaPath1, storageConfig)
		s.writeV2DeltaLog(ctx, schemapb.DataType_Int64, []int64{300, 400}, []int64{3002, 3003}, deltaPath2, storageConfig)

		newManifest, err := packed.AddDeltaLogsToManifest(manifestPath, storageConfig, []packed.DeltaLogEntry{
			{Path: deltaPath1, NumEntries: 2},
			{Path: deltaPath2, NumEntries: 2},
		})
		s.Require().NoError(err)

		segment := &datapb.CompactionSegmentBinlogs{Manifest: newManifest}
		pk2Ts, err := ComposeDeleteFromDeltalogs(ctx, schemapb.DataType_Int64, segment, options...)
		s.NoError(err)
		s.Equal(4, len(pk2Ts))
		s.Equal(typeutil.Timestamp(3000), pk2Ts[int64(100)])
		s.Equal(typeutil.Timestamp(3001), pk2Ts[int64(200)])
		s.Equal(typeutil.Timestamp(3002), pk2Ts[int64(300)])
		s.Equal(typeutil.Timestamp(3003), pk2Ts[int64(400)])
	})

	s.Run("V2 manifest - no deltalogs", func() {
		basePath := filepath.Join(dir, "insert_log/1/2/3_empty")
		manifestPath := s.createBaseManifest(basePath, storageConfig)

		segment := &datapb.CompactionSegmentBinlogs{Manifest: manifestPath}
		_, err := ComposeDeleteFromDeltalogs(ctx, schemapb.DataType_Int64, segment, options...)
		s.NoError(err)
	})
}
