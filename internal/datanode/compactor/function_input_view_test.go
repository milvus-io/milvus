// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"context"
	"errors"
	"io"
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type decodedTextBatchReader struct {
	records []storage.Record
	next    int
	closed  bool
	err     error
}

type capturingTextMaterializer struct {
	fieldID int64
	text    string
}

func (m *capturingTextMaterializer) Materialize(record storage.Record) (map[int64]arrow.Array, error) {
	m.text = record.Column(m.fieldID).(*array.String).Value(0)
	return nil, nil
}

func (m *capturingTextMaterializer) Close() {}

func (r *decodedTextBatchReader) Next() (storage.Record, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.next == len(r.records) {
		return nil, io.EOF
	}
	record := r.records[r.next]
	r.next++
	return record, nil
}

func (r *decodedTextBatchReader) Close() error {
	r.closed = true
	return nil
}

func TestFunctionInputPreparerAlignsIndependentBatchBoundaries(t *testing.T) {
	const textID, sparseID = int64(105), int64(106)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: sparseID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{
		Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{textID}, OutputFieldIds: []int64{sparseID},
	}}
	manifest := packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1)
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}}, manifest,
		&indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)

	first := newStringArray(t, []string{"alpha"})
	second := newStringArray(t, []string{"beta", "gamma"})
	defer first.Release()
	defer second.Release()
	reader := &decodedTextBatchReader{records: []storage.Record{
		&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: first}},
		&materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{textID: second}},
	}}
	preparer.newReader = func(context.Context) (storage.RecordReader, error) { return reader, nil }

	refsA := newBinaryArray(t, [][]byte{[]byte("ref-0"), []byte("ref-1")})
	refsB := newBinaryArray(t, [][]byte{[]byte("ref-2")})
	defer refsA.Release()
	defer refsB.Release()
	for i, test := range []struct {
		refs *array.Binary
		want []string
	}{
		{refs: refsA, want: []string{"alpha", "beta"}},
		{refs: refsB, want: []string{"gamma"}},
	} {
		base := &materializerTestRecord{len: test.refs.Len(), columns: map[storage.FieldID]arrow.Array{textID: test.refs}}
		logical, cleanup, prepareErr := preparer.Prepare(context.Background(), base)
		require.NoError(t, prepareErr, "primary batch %d", i)
		values := logical.Column(textID).(*array.String)
		for row, want := range test.want {
			require.Equal(t, want, values.Value(row))
		}
		require.Same(t, test.refs, base.Column(textID))
		cleanup()
	}
	require.NoError(t, preparer.Finish())
	require.NoError(t, preparer.Close())
	require.True(t, reader.closed)
}

func TestFunctionInputPreparerAdvancesPastFullyFilteredBatch(t *testing.T) {
	const textID, sparseID = int64(105), int64(106)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: sparseID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{
		Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{textID}, OutputFieldIds: []int64{sparseID},
	}}
	manifest := packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1)
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}}, manifest,
		&indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)
	first := newStringArray(t, []string{"discard-a", "discard-b"})
	second := newStringArray(t, []string{"keep"})
	defer first.Release()
	defer second.Release()
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		return &decodedTextBatchReader{records: []storage.Record{
			&materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{textID: first}},
			&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: second}},
		}}, nil
	}
	discardRefs := newBinaryArray(t, [][]byte{[]byte("ref-a"), []byte("ref-b")})
	keepRefs := newBinaryArray(t, [][]byte{[]byte("ref-c")})
	defer discardRefs.Release()
	defer keepRefs.Release()
	discarded := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{textID: discardRefs}}
	require.NoError(t, preparer.Skip(context.Background(), discarded))
	kept := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: keepRefs}}
	logical, cleanup, err := preparer.Prepare(context.Background(), kept)
	require.NoError(t, err)
	require.Equal(t, "keep", logical.Column(textID).(*array.String).Value(0))
	cleanup()
	require.NoError(t, preparer.Finish())
	require.NoError(t, preparer.Close())
}

func TestFunctionInputPreparerSkipsTextReadWhenOutputAlreadyExists(t *testing.T) {
	const textID, sparseID = int64(105), int64(106)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: sparseID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{
		Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{textID}, OutputFieldIds: []int64{sparseID},
	}}
	preparer, err := newFunctionInputPreparer(schema, functions,
		map[int64]struct{}{textID: {}, sparseID: {}}, "unused-manifest", nil)
	require.NoError(t, err)
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		t.Fatal("persisted function output must not trigger a TEXT read")
		return nil, nil
	}
	refs := newBinaryArray(t, [][]byte{[]byte("original-ref")})
	defer refs.Release()
	base := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}}
	logical, cleanup, err := preparer.Prepare(context.Background(), base)
	require.NoError(t, err)
	require.Same(t, base, logical)
	cleanup()
	require.NoError(t, preparer.Finish())
	require.NoError(t, preparer.Close())
}

func TestFunctionInputPreparerRejectsTrailingDecodedRows(t *testing.T) {
	const textID, sparseID = int64(105), int64(106)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: sparseID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{
		Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{textID}, OutputFieldIds: []int64{sparseID},
	}}
	manifest := packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1)
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}}, manifest,
		&indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)
	strings := newStringArray(t, []string{"matched", "extra"})
	defer strings.Release()
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		return &decodedTextBatchReader{records: []storage.Record{
			&materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{textID: strings}},
		}}, nil
	}
	refs := newBinaryArray(t, [][]byte{[]byte("ref")})
	defer refs.Release()
	base := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}}
	logical, cleanup, err := preparer.Prepare(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, "matched", logical.Column(textID).(*array.String).Value(0))
	cleanup()
	require.ErrorContains(t, preparer.Finish(), "trailing physical rows")
	require.NoError(t, preparer.Close())
}

func TestMaterializedRecordReaderUsesDecodedTextWithoutReplacingPhysicalRefs(t *testing.T) {
	const textID, sparseID = int64(105), int64(106)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: sparseID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{
		Name: "bm25", Type: schemapb.FunctionType_BM25,
		InputFieldIds: []int64{textID}, OutputFieldIds: []int64{sparseID},
	}}
	manifest := packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1)
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}}, manifest,
		&indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)
	strings := newStringArray(t, []string{"hello"})
	defer strings.Release()
	secondary := &decodedTextBatchReader{records: []storage.Record{
		&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: strings}},
	}}
	preparer.newReader = func(context.Context) (storage.RecordReader, error) { return secondary, nil }
	refs := newBinaryArray(t, [][]byte{[]byte("original-ref")})
	defer refs.Release()
	primary := &materializerTestReader{records: []*materializerTestRecord{
		{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}},
	}}
	input := &capturingTextMaterializer{fieldID: textID}
	materializer := &RecordMaterializer{schema: schema, materializers: []FunctionMaterializer{input}}
	reader := newMaterializedRecordReader(context.Background(), primary, materializer, preparer)
	record, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, "hello", input.text)
	require.Same(t, refs, record.Column(textID))
	require.NoError(t, reader.Close())
	require.True(t, secondary.closed)
}

func TestFunctionInputPreparerSharesTextBetweenFunctions(t *testing.T) {
	const textID = int64(105)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: 106, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		{FieldID: 107, Name: "hash", DataType: schemapb.DataType_BinaryVector},
	}}
	functions := []*schemapb.FunctionSchema{
		{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{textID}, OutputFieldIds: []int64{106}},
		{Type: schemapb.FunctionType_MinHash, InputFieldIds: []int64{textID}, OutputFieldIds: []int64{107}},
	}
	root := t.TempDir()
	manifest := packed.MarshalManifestPath(path.Join(root, "insert_log/1/2/3"), 1)
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}}, manifest,
		&indexpb.StorageConfig{StorageType: "local", RootPath: root})
	require.NoError(t, err)

	values := newStringArray(t, []string{"alpha"})
	defer values.Release()
	reader := &decodedTextBatchReader{records: []storage.Record{
		&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: values}},
		&materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: values}},
	}}
	constructCalls := 0
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		constructCalls++
		return reader, nil
	}

	refs := newBinaryArray(t, [][]byte{[]byte("opaque-reference")})
	defer refs.Release()
	base := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}}
	for i := 0; i < 2; i++ {
		logical, cleanup, prepareErr := preparer.Prepare(context.Background(), base)
		require.NoError(t, prepareErr)
		require.Equal(t, "alpha", logical.Column(textID).(*array.String).Value(0))
		require.Same(t, refs, base.Column(textID))
		cleanup()
	}
	require.Equal(t, 1, constructCalls)
	require.NoError(t, preparer.Finish())
	require.NoError(t, preparer.Close())
	require.True(t, reader.closed)
}

func TestFunctionInputPreparerReusesAlreadyDecodedText(t *testing.T) {
	const textID = int64(105)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: 106, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{textID}, OutputFieldIds: []int64{106}}}
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}},
		"unused-manifest", nil)
	require.NoError(t, err)
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		t.Fatal("already decoded TEXT must not open another LOB reader")
		return nil, nil
	}
	strings := newStringArray(t, []string{"a", "b"})
	defer strings.Release()
	base := &materializerTestRecord{len: 2, columns: map[storage.FieldID]arrow.Array{textID: strings}}
	logical, cleanup, err := preparer.Prepare(context.Background(), base)
	require.NoError(t, err)
	require.Same(t, base, logical)
	cleanup()
	require.NoError(t, preparer.Close())
}

func TestFunctionInputPreparerPreservesTransientDecodeError(t *testing.T) {
	const textID = int64(105)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
		{FieldID: 106, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	functions := []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{textID}, OutputFieldIds: []int64{106}}}
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}},
		packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1), &indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)
	reader := &decodedTextBatchReader{err: merr.WrapErrIoTooManyRequests("lob", errors.New("throttled"))}
	preparer.newReader = func(context.Context) (storage.RecordReader, error) {
		return reader, nil
	}
	refs := newBinaryArray(t, [][]byte{[]byte("opaque-reference")})
	defer refs.Release()
	base := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}}
	logical, cleanup, err := preparer.Prepare(context.Background(), base)
	require.Error(t, err)
	require.Nil(t, logical)
	cleanup()
	require.True(t, merr.IsRetryableErr(err), "transient decode error was reclassified: %v", err)
	require.Same(t, refs, base.Column(textID))
	require.NoError(t, preparer.Close())
}
