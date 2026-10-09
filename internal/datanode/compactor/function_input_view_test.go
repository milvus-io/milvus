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

type countingTextLOBDecoder struct {
	values      *array.String
	decodeCalls int
	closeCalls  int
	decodeErr   error
}

func (d *countingTextLOBDecoder) Decode(_ context.Context, _ *array.Binary) (*array.String, error) {
	d.decodeCalls++
	if d.decodeErr != nil {
		return nil, d.decodeErr
	}
	d.values.Retain()
	return d.values, nil
}

func (d *countingTextLOBDecoder) Close() error {
	d.closeCalls++
	return nil
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
	fake := &countingTextLOBDecoder{values: values}
	constructCalls := 0
	preparer.newDecoder = func(fieldID int64, lobBase string, _ *indexpb.StorageConfig) (textLOBDecoder, error) {
		constructCalls++
		require.Equal(t, textID, fieldID)
		require.Equal(t, path.Join(root, "insert_log/1/2/lobs/105"), lobBase)
		return fake, nil
	}

	refs := newBinaryArray(t, [][]byte{[]byte("opaque-reference")})
	defer refs.Release()
	base := &materializerTestRecord{len: 1, columns: map[storage.FieldID]arrow.Array{textID: refs}}
	for i := 0; i < 2; i++ {
		logical, cleanup, prepareErr := preparer.Prepare(context.Background(), base)
		require.NoError(t, prepareErr)
		require.Same(t, values, logical.Column(textID))
		require.Same(t, refs, base.Column(textID))
		cleanup()
	}
	require.Equal(t, 1, constructCalls)
	require.Equal(t, 2, fake.decodeCalls)
	require.NoError(t, preparer.Close())
	require.Equal(t, 1, fake.closeCalls)
}

func TestFunctionInputPreparerReusesAlreadyDecodedText(t *testing.T) {
	const textID = int64(105)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: textID, Name: "doc", DataType: schemapb.DataType_Text},
	}}
	functions := []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{textID}}}
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}},
		"unused-manifest", nil)
	require.NoError(t, err)
	preparer.newDecoder = func(int64, string, *indexpb.StorageConfig) (textLOBDecoder, error) {
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
	}}
	functions := []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{textID}}}
	preparer, err := newFunctionInputPreparer(schema, functions, map[int64]struct{}{textID: {}},
		packed.MarshalManifestPath(path.Join(t.TempDir(), "insert_log/1/2/3"), 1), &indexpb.StorageConfig{StorageType: "local"})
	require.NoError(t, err)
	decoder := &countingTextLOBDecoder{decodeErr: merr.WrapErrIoTooManyRequests("lob", errors.New("throttled"))}
	preparer.newDecoder = func(int64, string, *indexpb.StorageConfig) (textLOBDecoder, error) {
		return decoder, nil
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
