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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestManifestTextSizeAcrossWriterRepresentations(t *testing.T) {
	paramtable.Init()
	for _, format := range []string{"parquet", "vortex"} {
		for _, sharedGroup := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/shared=%t", format, sharedGroup), func(t *testing.T) {
				ctx := context.Background()
				cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text, Nullable: true},
					{FieldID: 102, Name: "other_text", DataType: schemapb.DataType_Text, Nullable: true},
				}}
				groups := []storagecommon.ColumnGroup{
					{GroupID: 0, Columns: []int{0}, Fields: []int64{100}},
					{GroupID: 1, Columns: []int{1}, Fields: []int64{101}},
					{GroupID: 2, Columns: []int{2}, Fields: []int64{102}},
				}
				targetGroup := int64(1)
				if sharedGroup {
					groups = []storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{100, 101, 102}}}
					targetGroup = 0
				}
				base := filepath.Join(cfg.RootPath, "raw")
				texts := []packed.TextColumnConfig{
					{FieldID: 101, LobBasePath: filepath.Join(cfg.RootPath, "lobs/101"), InlineThreshold: 64, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1 << 20},
					{FieldID: 102, LobBasePath: filepath.Join(cfg.RootPath, "lobs/102"), InlineThreshold: 64, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1 << 20},
				}
				raw, err := NewPackedTextBatchWriter("", base, schema, 0, 0, groups, cfg, texts, nil, format, nil)
				require.NoError(t, err)
				arrowSchema, err := ConvertToArrowSchema(schema, true)
				require.NoError(t, err)
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
				defer builder.Release()
				builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
				builder.Field(1).(*array.StringBuilder).AppendValues([]string{"inline", "", "", strings.Repeat("text", 32<<10), strings.Repeat("more", 16<<10)}, []bool{true, true, false, true, true})
				builder.Field(2).(*array.StringBuilder).AppendValues([]string{"other inline", "", "", strings.Repeat("other", 64<<10), ""}, []bool{true, true, false, true, true})
				record := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{100: 0, 101: 1, 102: 2})
				require.NoError(t, raw.Write(record))
				record.Release()
				rawStats := raw.GetColumnGroupWrittenUncompressed(targetGroup)
				output, err := raw.Close()
				require.NoError(t, err)
				defer output.Destroy()
				rawManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(base, packed.ManifestEarliest), cfg, &packed.ManifestUpdates{NewFiles: output})
				require.NoError(t, err)

				// This is the ordinary sort/REUSE_ALL writer: its input retains
				// the same LOB references, so its Go statistics count encoded bytes.
				refsBase := filepath.Join(cfg.RootPath, "references")
				refs, err := newPackedRecordBatchWriter(refsBase, schema, 0, 0, groups, cfg, nil, true, true, format, nil)
				require.NoError(t, err)
				reader, err := packed.NewFFIPackedReader(rawManifest, overrideTextFieldsToBinary(schema, arrowSchema), []string{"100", "101", "102"}, 1<<20, cfg, nil, packed.ExternalReaderContext{})
				require.NoError(t, err)
				defer reader.Close()
				for {
					batch, err := reader.ReadNext()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					record := NewSimpleArrowRecord(batch, map[FieldID]int{100: 0, 101: 1, 102: 2})
					require.NoError(t, refs.Write(record))
					record.Release()
				}
				refStats := refs.GetColumnGroupWrittenUncompressed(targetGroup)
				require.Greater(t, rawStats, refStats*100, "fixture must expose the two persisted size conventions")
				refOutput, err := refs.Close()
				require.NoError(t, err)
				defer refOutput.Destroy()
				refManifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(refsBase, packed.ManifestEarliest), cfg, &packed.ManifestUpdates{NewFiles: refOutput})
				require.NoError(t, err)
				lobs, err := packed.GetManifestLobFiles(rawManifest, cfg)
				require.NoError(t, err)
				refManifest, err = packed.AddLobFilesToTransaction(refManifest, cfg, lobs)
				require.NoError(t, err)
				manifestIO := packed.NewManifestIOContext(1)
				defer manifestIO.Close()
				rawSize, found, err := packed.EstimateManifestTextInputSize(ctx, manifestIO, rawManifest, 101, cfg, nil)
				require.NoError(t, err)
				refSize, refsFound, err := packed.EstimateManifestTextInputSize(ctx, manifestIO, refManifest, 101, cfg, nil)
				require.NoError(t, err)
				require.True(t, found && refsFound)
				// Parquet readback can drop the all-valid PK bitmap (one byte
				// for these five rows); the logical payload is still identical.
				require.InDelta(t, rawSize, refSize, 1, "equivalent encoded data must not depend on writer input representation")
				var targetLobs int64
				for _, lob := range lobs {
					if lob.FieldID == 101 {
						targetLobs += lob.FileSizeBytes
					}
				}
				require.Greater(t, rawSize, targetLobs, "inline values and encoded references are still charged")
				require.Less(t, rawSize, int64(rawStats)+targetLobs, "logical TEXT must not be charged twice")
				missingSize, found, err := packed.EstimateManifestTextInputSize(ctx, manifestIO, rawManifest, 999, cfg, nil)
				require.NoError(t, err)
				require.Zero(t, missingSize)
				require.False(t, found)
				if format == "parquet" && !sharedGroup {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					_, _, err := packed.EstimateManifestTextInputSize(canceled, manifestIO, rawManifest, 101, cfg, nil)
					require.ErrorIs(t, err, context.Canceled)

					// No cipher plugin is loaded in this test process. Supplying
					// a context must reach the native reader and fail explicitly.
					_, _, err = packed.EstimateManifestTextInputSize(ctx, manifestIO, rawManifest, 101, cfg,
						&indexcgopb.StoragePluginContext{CollectionId: 1, EncryptionZoneId: 2, EncryptionKey: "required-key"})
					require.ErrorContains(t, err, "cipher plugin")

					fragments, err := packed.ReadFragmentsFromManifest(rawManifest, cfg, []string{"101"})
					require.NoError(t, err)
					require.NotEmpty(t, fragments)
					require.NoError(t, os.Remove(fragments[0].FilePath))
					_, _, err = packed.EstimateManifestTextInputSize(ctx, manifestIO, rawManifest, 101, cfg, nil)
					require.Error(t, err, "unavailable footer must not return an estimate from ambiguous Go statistics")
				}
			})
		}
	}
}
