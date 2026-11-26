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

package packed

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory/mallocator"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFFISegmentWriterTextLogicalBytes(t *testing.T) {
	paramtable.Init()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	fields := []arrow.Field{
		{Name: "1008", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{ArrowFieldIdMetadataKey: "1008"})},
		{Name: "402", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{ArrowFieldIdMetadataKey: "402"})},
	}
	schema := arrow.NewSchema(fields, nil)
	textConfigs := []TextColumnConfig{
		{FieldID: 402, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "402"), InlineThreshold: 65536},
		{FieldID: 1008, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "1008"), InlineThreshold: 65536},
	}
	writer := func(name string, schema *arrow.Schema, columns []TextColumnConfig) *FFISegmentWriter {
		w, err := NewFFISegmentWriter(schema, &SegmentWriterConfig{SegmentPath: filepath.Join(cfg.RootPath, name), TextColumns: columns}, cfg, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			if !w.closed {
				if out, _ := w.Close(); out != nil {
					out.Destroy()
				}
			}
		})
		return w
	}
	metrics := func() storagev2.FilesystemMetrics {
		m, err := storagev2.GetFilesystemMetricsWithConfig(cfg)
		require.NoError(t, err)
		require.Contains(t, m, "origin")
		return m["origin"]
	}
	closeOutput := func(w *FFISegmentWriter, rows int64, want map[int64]uint64) *SegmentOutput {
		before := metrics()
		o, err := w.Close()
		require.NoError(t, err)
		t.Cleanup(o.Destroy)
		out := o.(*SegmentOutput)
		require.Equal(t, rows, out.RowsWritten())
		require.Equal(t, want, out.TextLogicalBytes())
		after := metrics()
		require.Equal(t, before.ReadCount, after.ReadCount, "close and stats lookup must not read files")
		require.Equal(t, before.ReadBytes, after.ReadBytes)
		return out
	}

	// The unused first and last rows must not contribute, even though Arrow
	// slices retain the entire input buffer. Field 402 is materialized but all NULL.
	builder := array.NewRecordBuilder(mallocator.NewMallocator(), schema)
	defer builder.Release()
	body := builder.Field(0).(*array.StringBuilder)
	for i, value := range []string{"discard", "猫", "", "", strings.Repeat("a", 23), strings.Repeat("b", 65535), strings.Repeat("c", 65536), strings.Repeat("d", 65537), "discard"} {
		if i == 3 {
			body.AppendNull()
		} else {
			body.Append(value)
		}
		builder.Field(1).AppendNull()
	}
	record := builder.NewRecord()
	defer record.Release()
	w := writer("source", schema, textConfigs)
	start := metrics()
	for _, bounds := range [][2]int64{{1, 4}, {4, 8}} {
		slice := record.NewSlice(bounds[0], bounds[1])
		err := w.Write(slice)
		slice.Release()
		require.NoError(t, err)
		require.NoError(t, w.SyncBuffered())
	}
	source := closeOutput(w, 7, map[int64]uint64{1008: 3 + 23 + 65535 + 65536 + 65537, 402: 0})
	end := metrics()
	require.Equal(t, start.ReadCount, end.ReadCount)
	require.Equal(t, start.ReadBytes, end.ReadBytes)
	t.Logf("normal write: %d LOB reads, %d bytes", end.ReadCount-start.ReadCount, end.ReadBytes-start.ReadBytes)
	manifest, err := CommitManifestUpdates(MarshalManifestPath(filepath.Join(cfg.RootPath, "source"), ManifestEarliest), cfg, &ManifestUpdates{NewFiles: source})
	require.NoError(t, err)

	// Read only the references. Native REWRITE_ALL will read the retained
	// external body itself. The other column is passed through, not encoded.
	for i := range fields {
		fields[i].Type = arrow.BinaryTypes.Binary
	}
	refSchema := arrow.NewSchema(fields, nil)
	r, err := NewFFIPackedReader(manifest, refSchema, []string{"1008", "402"}, 4<<20, cfg, nil, ExternalReaderContext{})
	require.NoError(t, err)
	defer r.Close()
	refs, err := r.ReadNext()
	require.NoError(t, err)
	defer refs.Release()
	require.EqualValues(t, 7, refs.NumRows())
	encoded := refs.Column(0).(*array.Binary)
	require.Len(t, encoded.Value(3), 24) // 23-byte inline body
	require.Len(t, encoded.Value(5), 24) // 65,536-byte external body
	rewrite := textConfigs[1]
	rewrite.RewriteMode = true
	rewrite.InlineThreshold = 24 // also externalize the previously inline 65,535-byte value
	w = writer("rewrite", refSchema, []TextColumnConfig{rewrite})
	kept := refs.NewSlice(1, 6)
	defer kept.Release()
	start = metrics()
	require.NoError(t, w.Write(kept))
	out := closeOutput(w, 5, map[int64]uint64{1008: 23 + 65535 + 65536})
	end = metrics()
	require.Greater(t, end.ReadCount, start.ReadCount, "rewrite must really read an external body")
	require.Greater(t, end.ReadBytes, start.ReadBytes)
	t.Logf("native rewrite: %d LOB reads, %d bytes", end.ReadCount-start.ReadCount, end.ReadBytes-start.ReadBytes)
	out.Destroy()
	snapshot := out.TextLogicalBytes()
	delete(snapshot, 1008)
	require.Equal(t, map[int64]uint64{1008: 23 + 65535 + 65536}, out.TextLogicalBytes())
}

func TestFFISegmentWriterTextBytesAfterFailedWrite(t *testing.T) {
	paramtable.Init()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "body", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{ArrowFieldIdMetadataKey: "104"})},
		{Name: "refs", Type: arrow.BinaryTypes.Binary, Metadata: arrow.MetadataFrom(map[string]string{ArrowFieldIdMetadataKey: "123"})},
	}, nil)
	w, err := NewFFISegmentWriter(schema, &SegmentWriterConfig{
		SegmentPath: filepath.Join(cfg.RootPath, "segment"),
		TextColumns: []TextColumnConfig{
			{FieldID: 104, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "104")},
			{FieldID: 123, LobBasePath: filepath.Join(cfg.RootPath, "lobs", "123"), RewriteMode: true},
		},
	}, cfg, nil)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(mallocator.NewMallocator(), schema)
	defer builder.Release()
	write := func(ref []byte) error {
		builder.Field(0).(*array.StringBuilder).Append("body")
		builder.Field(1).(*array.BinaryBuilder).Append(ref)
		record := builder.NewRecord()
		defer record.Release()
		return w.Write(record)
	}
	require.NoError(t, write([]byte{0, 'x'}))
	// The first encoder advances, but decoding the second column fails before
	// the segment accepts the batch. Preserve the write error; closing for
	// cleanup must not make the first counter look complete for the one row.
	require.ErrorContains(t, write([]byte{1}), "invalid LOB reference size")
	output, err := w.Close()
	require.NoError(t, err)
	defer output.Destroy()
	out := output.(*SegmentOutput)
	require.EqualValues(t, 1, out.RowsWritten())
	require.Equal(t, map[int64]uint64{123: 1}, out.TextLogicalBytes())
}
