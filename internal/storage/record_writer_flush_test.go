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
	"path"
	"sort"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

const flushTestTextFieldID = int64(101)

// newTextFlushTestWriter builds a TEXT-aware writer whose unflushed-input bound
// is bufferSize. The TEXT column keeps every value out of line (inline
// threshold 1), so a flush has real LOB work to do.
func newTextFlushTestWriter(t *testing.T, bufferSize int64) (*packedTextBatchWriter, *schemapb.CollectionSchema, string, *indexpb.StorageConfig) {
	t.Helper()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: flushTestTextFieldID, Name: "text", DataType: schemapb.DataType_Text},
	}}
	rootPath := t.TempDir()
	segmentPath := SegmentManifestBasePath(rootPath, 1, 2, 3)
	config := &indexpb.StorageConfig{StorageType: "local", RootPath: rootPath}
	columnGroups := []storagecommon.ColumnGroup{{
		GroupID: 0,
		Columns: []int{0, 1, 2, 3},
		Fields:  []int64{common.RowIDField, common.TimeStampField, 100, flushTestTextFieldID},
	}}
	textConfig := []packed.TextColumnConfig{{
		FieldID:             flushTestTextFieldID,
		LobBasePath:         path.Join(SegmentPartitionBasePath(rootPath, 1, 2), "lobs", "101"),
		InlineThreshold:     1,
		MaxLobFileBytes:     1 << 20,
		FlushThresholdBytes: 1,
	}}

	w, err := NewPackedTextBatchWriter("", segmentPath, schema, bufferSize, 0,
		columnGroups, config, textConfig, nil, "parquet", []string{"parquet"})
	require.NoError(t, err)
	return w, schema, segmentPath, config
}

func newTextFlushTestRecord(t *testing.T, schema *schemapb.CollectionSchema, rowIDs []int64, texts []string) Record {
	t.Helper()
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	timestamps := make([]int64, len(rowIDs))
	for i := range timestamps {
		timestamps[i] = int64(i + 1)
	}
	builder.Field(0).(*array.Int64Builder).AppendValues(rowIDs, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(timestamps, nil)
	builder.Field(2).(*array.Int64Builder).AppendValues(rowIDs, nil)
	builder.Field(3).(*array.StringBuilder).AppendValues(texts, nil)

	return NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		100:                   2,
		flushTestTextFieldID:  3,
	})
}

// TestPackedTextBatchWriterFlushesUnflushedInput drives the mid-stream flush
// branch: a bound of one byte means every written record crosses it, so each
// Write must flush and the writer must keep accepting records afterwards.
func TestPackedTextBatchWriterFlushesUnflushedInput(t *testing.T) {
	w, schema, _, _ := newTextFlushTestWriter(t, 1)

	flushes := 0
	patch := mockey.Mock((*packed.FFISegmentWriter).SyncBuffered).To(func(_ *packed.FFISegmentWriter) error {
		flushes++
		return nil
	}).Build()
	defer patch.UnPatch()

	first := newTextFlushTestRecord(t, schema, []int64{1, 2}, []string{"first text", "second text"})
	defer first.Release()
	require.NoError(t, w.Write(first))
	assert.Equal(t, 1, flushes, "first record crosses the bound")

	second := newTextFlushTestRecord(t, schema, []int64{3, 4}, []string{"third text", "fourth text"})
	defer second.Release()
	require.NoError(t, w.Write(second))
	assert.Equal(t, 2, flushes, "writer keeps working after a mid-stream flush")
	assert.Equal(t, int64(4), w.GetWrittenRowNum())

	output, err := w.Close()
	require.NoError(t, err)
	if output != nil {
		output.Destroy()
	}
}

func TestPackedTextBatchWriterPropagatesFlushError(t *testing.T) {
	w, schema, _, _ := newTextFlushTestWriter(t, 1)

	flushErr := errors.New("flush failed")
	patch := mockey.Mock((*packed.FFISegmentWriter).SyncBuffered).Return(flushErr).Build()
	defer patch.UnPatch()

	record := newTextFlushTestRecord(t, schema, []int64{1}, []string{"first text"})
	defer record.Release()
	require.ErrorIs(t, w.Write(record), flushErr)

	if output, closeErr := w.Close(); closeErr == nil && output != nil {
		output.Destroy()
	}
}

// TestPackedTextBatchWriterKeepsOutputAcrossMidStreamFlushes writes two records
// that each force a real flush, then reads the committed manifest back to prove
// the flushed column groups and LOB files still form one complete segment.
func TestPackedTextBatchWriterKeepsOutputAcrossMidStreamFlushes(t *testing.T) {
	w, schema, segmentPath, config := newTextFlushTestWriter(t, 1)

	wantTexts := []string{"first text", "second text", "third text", "fourth text"}
	for _, batch := range []struct {
		rowIDs []int64
		texts  []string
	}{
		{[]int64{1, 2}, wantTexts[:2]},
		{[]int64{3, 4}, wantTexts[2:]},
	} {
		record := newTextFlushTestRecord(t, schema, batch.rowIDs, batch.texts)
		require.NoError(t, w.Write(record))
		record.Release()
	}

	output, err := w.Close()
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Destroy()
	require.Equal(t, int64(len(wantTexts)), w.GetWrittenRowNum())

	manifest, err := packed.CommitManifestUpdates(packed.MarshalManifestPath(segmentPath, packed.ManifestEarliest), config,
		&packed.ManifestUpdates{NewFiles: output})
	require.NoError(t, err)

	reader, err := NewManifestRecordReader(context.Background(), manifest, schema,
		WithVersion(StorageV3), WithStorageConfig(config), WithResolveTextLob())
	require.NoError(t, err)
	defer reader.Close()

	var gotTexts []string
	for {
		record, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		textColumn, ok := record.Column(flushTestTextFieldID).(*array.String)
		require.True(t, ok)
		for i := 0; i < textColumn.Len(); i++ {
			gotTexts = append(gotTexts, textColumn.Value(i))
		}
	}
	sort.Strings(gotTexts)
	sort.Strings(wantTexts)
	assert.Equal(t, wantTexts, gotTexts)
}
