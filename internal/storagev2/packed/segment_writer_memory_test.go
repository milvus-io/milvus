// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/memory/mallocator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func textRetentionSchema() *arrow.Schema {
	field := func(id string, typ arrow.DataType, nullable bool) arrow.Field {
		return arrow.Field{
			Name: id, Type: typ, Nullable: nullable,
			Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{id}),
		}
	}
	return arrow.NewSchema([]arrow.Field{
		field("100", arrow.PrimitiveTypes.Int64, false),
		field("101", arrow.BinaryTypes.String, true),
		field("102", arrow.BinaryTypes.String, true),
		field("103", &arrow.FixedSizeBinaryType{ByteWidth: 16}, false),
		field("104", arrow.BinaryTypes.Binary, true),
		field("105", arrow.ListOf(arrow.PrimitiveTypes.Int32), true),
	}, nil)
}

// Give TEXT its own allocator so the assertion measures live input buffers,
// independently of allocator caches, process RSS, and the writer's counters.
func textRetentionRecord(schema *arrow.Schema, textMemory, otherMemory memory.Allocator, start, rows int64) arrow.Record {
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		allocator := otherMemory
		if i == 1 || i == 2 {
			allocator = textMemory
		}
		builders[i] = array.NewBuilder(allocator, field.Type)
		defer builders[i].Release()
	}
	for i := int64(0); i < rows; i++ {
		id := start + i
		builders[0].(*array.Int64Builder).Append(id)
		for column := 1; column <= 2; column++ {
			text := builders[column].(*array.StringBuilder)
			switch (id + int64(column)) % 6 {
			case 0:
				text.AppendNull()
			case 1:
				text.Append("")
			case 2:
				text.Append(fmt.Sprintf("inline-%d-%d", column, id))
			case 3:
				text.Append(fmt.Sprintf("中文-%d-%d-", column, id) + strings.Repeat("文本", 32<<10))
			default:
				text.Append(fmt.Sprintf("lob-%d-%d-", column, id) + strings.Repeat("x", 128<<10))
			}
		}
		var vector [16]byte
		for dimension := 0; dimension < 4; dimension++ {
			binary.LittleEndian.PutUint32(vector[dimension*4:], math.Float32bits(float32(id)+float32(dimension)/4))
		}
		builders[3].(*array.FixedSizeBinaryBuilder).Append(vector[:])
		json := builders[4].(*array.BinaryBuilder)
		if id%3 == 0 {
			json.AppendNull()
		} else {
			json.Append([]byte(fmt.Sprintf(`{"id":%d,"kind":"retention"}`, id)))
		}
		list := builders[5].(*array.ListBuilder)
		if id%3 == 0 {
			list.AppendNull()
		} else {
			list.Append(true)
			if id%3 == 2 {
				values := list.ValueBuilder().(*array.Int32Builder)
				values.Append(int32(id))
				values.AppendNull()
				values.Append(int32(id + 1))
			}
		}
	}
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
		defer columns[i].Release()
	}
	return array.NewRecord(schema, columns, rows)
}

// Exercise the real Go exporter and C++ SegmentWriter. A scalar Parquet tail
// must remain buffered while raw TEXT becomes releasable after every Write,
// regardless of whether the caller also explicitly flushes the writer.
func TestFFISegmentWriterTextBuffersReleased(t *testing.T) {
	paramtable.Init()
	for _, flushEachBatch := range []bool{false, true} {
		for _, sliced := range []bool{false, true} {
			t.Run(fmt.Sprintf("flush=%t/sliced=%t", flushEachBatch, sliced), func(t *testing.T) {
				const batches, rows = 8, int64(8)
				// C++ keeps ordinary columns past the cgo call. Allocate the
				// exported buffers in C so they remain valid across Go GC.
				textMemory := memory.NewCheckedAllocator(mallocator.NewMallocator())
				otherMemory := memory.NewCheckedAllocator(mallocator.NewMallocator())
				defer textMemory.AssertSize(t, 0)
				defer otherMemory.AssertSize(t, 0)
				schema := textRetentionSchema()
				cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
				basePath := filepath.Join(cfg.RootPath, "segment")
				textColumns := []TextColumnConfig{
					{FieldID: 101, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "101"), InlineThreshold: 256, MaxLobFileBytes: 64 << 20, FlushThresholdBytes: 16 << 20},
					{FieldID: 102, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "102"), InlineThreshold: 256, MaxLobFileBytes: 64 << 20, FlushThresholdBytes: 16 << 20},
				}
				writer, err := NewFFISegmentWriter(schema, &SegmentWriterConfig{
					SegmentPath: basePath, TextColumns: textColumns,
					WriterFormat: "parquet", SchemaBasedPattern: "100,101,102,103,104,105",
				}, cfg, nil)
				require.NoError(t, err)
				defer func() {
					if !writer.closed {
						output, closeErr := writer.Close()
						assert.NoError(t, closeErr)
						if output != nil {
							output.Destroy()
						}
					}
				}()
				for batch := 0; batch < batches; batch++ {
					start := 1000 + int64(batch)*rows
					var record arrow.Record
					if sliced {
						original := textRetentionRecord(schema, textMemory, otherMemory, start-1, rows+2)
						record = original.NewSlice(1, rows+1)
						original.Release()
					} else {
						record = textRetentionRecord(schema, textMemory, otherMemory, start, rows)
					}
					err = writer.Write(record)
					record.Release()
					require.NoError(t, err)
					t.Logf("batch %d after Write: original TEXT=%d bytes, ordinary=%d bytes", batch, textMemory.CurrentAlloc(), otherMemory.CurrentAlloc())
					assert.Zero(t, textMemory.CurrentAlloc(), "batch %d: original TEXT is retained after Write", batch)
					assert.Positive(t, otherMemory.CurrentAlloc(), "fixture must retain an ordinary-column tail")
					if flushEachBatch {
						require.NoError(t, writer.SyncBuffered())
						t.Logf("batch %d after SyncBuffered: original TEXT=%d bytes, ordinary=%d bytes", batch, textMemory.CurrentAlloc(), otherMemory.CurrentAlloc())
						assert.Zero(t, textMemory.CurrentAlloc(), "batch %d: original TEXT is retained after flush", batch)
						assert.Positive(t, otherMemory.CurrentAlloc(), "flush must leave an ordinary-column tail in this fixture")
					}
				}
				output, err := writer.Close()
				require.NoError(t, err)
				defer output.Destroy()
				assert.Equal(t, int64(batches)*rows, output.(*SegmentOutput).RowsWritten())
				assert.Zero(t, textMemory.CurrentAlloc())
				assert.Zero(t, otherMemory.CurrentAlloc())
				manifest, err := CommitManifestUpdates(MarshalManifestPath(basePath, ManifestEarliest), cfg, &ManifestUpdates{NewFiles: output})
				require.NoError(t, err)
				reader, err := NewFFISegmentReader(manifest, schema, []string{"100", "101", "102", "103", "104", "105"}, 4<<20, cfg, textColumns)
				require.NoError(t, err)
				defer reader.Close()
				// A new allocation prevents the expected values from retaining input.
				expected := textRetentionRecord(schema, memory.DefaultAllocator, memory.DefaultAllocator, 1000, int64(batches)*rows)
				defer expected.Release()
				var seen int64
				for {
					actual, err := reader.ReadNext()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					func() {
						defer actual.Release()
						require.LessOrEqual(t, seen+actual.NumRows(), expected.NumRows())
						want := expected.NewSlice(seen, seen+actual.NumRows())
						defer want.Release()
						require.Equal(t, want.NumCols(), actual.NumCols())
						for column := 0; column < int(want.NumCols()); column++ {
							assert.True(t, array.Equal(want.Column(column), actual.Column(column)), "column %d at row %d differs after readback", column, seen)
						}
						seen += actual.NumRows()
					}()
				}
				assert.Equal(t, expected.NumRows(), seen)
			})
		}
	}
}
