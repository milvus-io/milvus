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

package importv2

import (
	"context"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The binlog struct evolution test restores segments of a collection whose
// struct gained and lost sub-fields after the segments were written, the way
// a backup restore does: the target collection keeps the source field IDs, the
// segments are read by the binlog import reader (StorageV1/V2 backup layout,
// StorageV3 manifest of a snapshot-source import), and the result must equal
// what insert stores for the same logical rows under the target schema:
//   - a sub-field added after the segment was written (scalar and vector,
//     element-nullable) gets one null element per element of the row;
//   - the stored column of a dropped sub-field (scalar and vector) is ignored.

const (
	evolutionAddedVecID = int64(212) // element-nullable vector sub-field added after the segment was written
	evolutionCollection = int64(1)
	evolutionPartition  = int64(2)
)

type binlogEvolutionCase struct {
	name        string
	version     int64
	writeSchema *schemapb.CollectionSchema
	readSchema  *schemapb.CollectionSchema
	rows        []parityRow
}

// evolutionRowsWithStruct drops null struct rows, which a non-nullable struct
// cannot hold.
func evolutionRowsWithStruct(rows []parityRow) []parityRow {
	out := make([]parityRow, 0, len(rows))
	for _, row := range rows {
		if row.s != nil {
			out = append(out, row)
		}
	}
	return out
}

func evolutionAddedSubFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{
		parityScalarSubField(parityExtraID, "extra", schemapb.DataType_Int64, true),
		parityVectorSubField(evolutionAddedVecID, "extra_emb", true),
	}
}

// evolutionPackedCase builds a StorageV2/V3 case: the segment holds every base
// sub-field plus a dropped scalar and a dropped vector sub-field.
func evolutionPackedCase(name string, version int64, nullableStruct bool) binlogEvolutionCase {
	write := paritySchema(append(parityBaseSubFields(),
		parityScalarSubField(parityDroppedID, "dropped", schemapb.DataType_Int64, true),
		parityVectorSubField(parityDroppedVecID, "dropped_emb", true)))
	read := paritySchema(append(parityBaseSubFields(), evolutionAddedSubFields()...))
	rows := parityRows()
	if !nullableStruct {
		write = parityNonNullableStruct(write)
		read = parityNonNullableStruct(read)
		rows = evolutionRowsWithStruct(rows)
	}
	for _, row := range rows {
		for j, elem := range row.s {
			elem["dropped"] = int64(1000 + j)
			elem["dropped_emb"] = []float32{float32(j), 0, 0, 1}
		}
	}
	return binlogEvolutionCase{name: name, version: version, writeSchema: write, readSchema: read, rows: rows}
}

// evolutionV1Case builds the StorageV1 case. StorageV1 cannot store
// element-nullable or nested Arrays nor nullable vector arrays, so the segment
// holds a non-nullable struct with legacy sub-fields only.
func evolutionV1Case() binlogEvolutionCase {
	legacy := func() []*schemapb.FieldSchema {
		return []*schemapb.FieldSchema{
			parityScalarSubField(201, "id", schemapb.DataType_Int64, false),
			parityVectorSubField(210, "emb2", false),
		}
	}
	write := parityNonNullableStruct(paritySchema(append(legacy(),
		parityScalarSubField(parityDroppedID, "dropped", schemapb.DataType_Int64, false))))
	read := parityNonNullableStruct(paritySchema(append(legacy(), evolutionAddedSubFields()...)))
	rows := evolutionRowsWithStruct(parityRows())
	for i := range rows {
		elems := make([]parityElem, len(rows[i].s))
		for j, elem := range rows[i].s {
			elems[j] = parityElem{"id": elem["id"], "emb2": elem["emb2"], "dropped": int64(1000 + j)}
		}
		rows[i].s = elems
	}
	return binlogEvolutionCase{name: "v1", version: storage.StorageV1, writeSchema: write, readSchema: read, rows: rows}
}

// evolutionWriteSegment writes the rows as one segment with the production
// binlog writer and returns the segment's insert log prefix (StorageV1/V2) or
// its manifest (StorageV3).
func evolutionWriteSegment(t *testing.T, cfg *indexpb.StorageConfig, tc binlogEvolutionCase, segmentID int64) (string, string) {
	data := parityInsertData(t, tc.writeSchema, tc.rows)
	rowIDs := make([]int64, len(tc.rows))
	timestamps := make([]int64, len(tc.rows))
	for i := range tc.rows {
		rowIDs[i] = int64(i + 1)
		timestamps[i] = int64(100 + i)
	}
	data.Data[common.RowIDField] = &storage.Int64FieldData{Data: rowIDs}
	data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: timestamps}

	schema := typeutil.AppendSystemFields(tc.writeSchema)
	arrowSchema, err := storage.ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, data, schema))
	fields := typeutil.GetAllFieldSchemas(schema)
	field2Col := make(map[storage.FieldID]int, len(fields))
	for i, field := range fields {
		field2Col[field.GetFieldID()] = i
	}
	record := storage.NewSimpleArrowRecord(builder.NewRecord(), field2Col)
	defer record.Release()

	var uploaded []string
	upload := func(_ context.Context, kvs map[string][]byte) error {
		for key, value := range kvs {
			if err := os.MkdirAll(filepath.Dir(key), 0o700); err != nil {
				return err
			}
			if err := os.WriteFile(key, value, 0o600); err != nil {
				return err
			}
			uploaded = append(uploaded, key)
		}
		return nil
	}
	writer, err := storage.NewBinlogRecordWriter(context.Background(), evolutionCollection, evolutionPartition, segmentID, schema,
		allocator.NewLocalAllocator(1000, 100000), 1<<20, int64(len(tc.rows)),
		storage.WithVersion(tc.version), storage.WithStorageConfig(cfg), storage.WithUploader(upload))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()

	if tc.version == storage.StorageV3 {
		require.NotEmpty(t, manifest)
		return "", manifest
	}
	var insertLog string
	if tc.version == storage.StorageV1 {
		for _, key := range uploaded {
			if strings.Contains(key, "/"+common.SegmentInsertLogPath+"/") {
				insertLog = key
				break
			}
		}
	} else {
		for _, fieldBinlog := range logs {
			for _, log := range fieldBinlog.GetBinlogs() {
				insertLog = log.GetLogPath()
			}
		}
	}
	require.NotEmpty(t, insertLog)
	// <prefix>/<field or column group ID>/<log ID>
	return path.Dir(path.Dir(insertLog)), ""
}

func TestBinlogImportStructSubFieldEvolution(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	root := t.TempDir()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, root)
	t.Cleanup(func() {
		pt.Reset(pt.CommonCfg.StorageType.Key)
		pt.Reset(pt.LocalStorageCfg.Path.Key)
	})
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	const bufferSize = 64 * 1024 * 1024

	cases := []binlogEvolutionCase{
		evolutionV1Case(),
		evolutionPackedCase("v2_nullable_struct", storage.StorageV2, true),
		// The added vector sub-field of a non-nullable struct is not nullable,
		// so only its element nullability lets the StorageV2 check pass it.
		evolutionPackedCase("v2_non_nullable_struct", storage.StorageV2, false),
		evolutionPackedCase("v3_nullable_struct", storage.StorageV3, true),
		evolutionPackedCase("v3_non_nullable_struct", storage.StorageV3, false),
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prefix, manifest := evolutionWriteSegment(t, cfg, tc, int64(10+i))
			var reader parityReader
			var err error
			if tc.version == storage.StorageV3 {
				reader, err = binlog.NewStorageV3ManifestReader(ctx, cm, tc.readSchema, cfg, manifest,
					0, math.MaxUint64, bufferSize, binlog.SourceEncryption{}, nil, 0, nil)
			} else {
				reader, err = binlog.NewReader(ctx, cm, tc.readSchema, cfg, tc.version, []string{prefix},
					0, math.MaxUint64, bufferSize, "", nil)
			}
			require.NoError(t, err)
			got := parityReadAll(t, tc.readSchema, reader)
			want := parityInsertData(t, tc.readSchema, tc.rows)
			parityRequireSameInsertData(t, tc.readSchema, want, got)
			parityRequireSameRecord(t, tc.readSchema, want, got)
			for _, droppedID := range []int64{parityDroppedID, parityDroppedVecID} {
				require.NotContains(t, got.Data, droppedID, "dropped sub-field %d must not be read", droppedID)
			}
		})
	}
}
