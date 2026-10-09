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
	"context"
	"path"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTextLOBDecoderPreservesRefsAndLogicalText(t *testing.T) {
	root := t.TempDir()
	decoder, err := NewTextLOBDecoder(105, path.Join(root, "insert_log/1/2/lobs/105"),
		&indexpb.StorageConfig{StorageType: "local", RootPath: root})
	require.NoError(t, err)

	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append([]byte{0, 'a', 'l', 'p', 'h', 'a'})
	builder.AppendNull()
	builder.Append([]byte{0})
	refs := builder.NewBinaryArray()
	defer refs.Release()

	texts, err := decoder.Decode(context.Background(), refs)
	require.NoError(t, err)
	defer texts.Release()
	require.Equal(t, 3, texts.Len())
	require.Equal(t, "alpha", texts.Value(0))
	require.True(t, texts.IsNull(1))
	require.Equal(t, "", texts.Value(2))
	require.Equal(t, []byte{0, 'a', 'l', 'p', 'h', 'a'}, refs.Value(0))
	require.Equal(t, []byte{0}, refs.Value(2))
	require.NoError(t, decoder.Close())
	require.NoError(t, decoder.Close())
	require.Equal(t, "alpha", texts.Value(0))
	closedResult, err := decoder.Decode(context.Background(), refs)
	require.Error(t, err)
	require.Nil(t, closedResult)
}

func TestTextLOBDecoderRejectsMalformedReference(t *testing.T) {
	root := t.TempDir()
	decoder, err := NewTextLOBDecoder(105, path.Join(root, "lobs/105"),
		&indexpb.StorageConfig{StorageType: "local", RootPath: root})
	require.NoError(t, err)
	defer decoder.Close()

	negativeOffset := make([]byte, 24)
	negativeOffset[0] = 1
	negativeOffset[20] = 0xff
	negativeOffset[21] = 0xff
	negativeOffset[22] = 0xff
	negativeOffset[23] = 0xff
	for name, ref := range map[string][]byte{
		"empty":           {},
		"unknown tag":     {2, 'x'},
		"short LOB":       {1},
		"negative offset": negativeOffset,
	} {
		t.Run(name, func(t *testing.T) {
			builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			defer builder.Release()
			builder.Append(ref)
			refs := builder.NewBinaryArray()
			defer refs.Release()
			texts, err := decoder.Decode(context.Background(), refs)
			require.Error(t, err)
			require.Nil(t, texts)
			require.True(t, merr.IsSegcoreDataFormatBroken(err), "expected corruption code, got %v", err)
		})
	}
}

func TestTextLOBDecoderRejectsCanceledContextBeforeNativeRead(t *testing.T) {
	root := t.TempDir()
	decoder, err := NewTextLOBDecoder(105, path.Join(root, "lobs/105"),
		&indexpb.StorageConfig{StorageType: "local", RootPath: root})
	require.NoError(t, err)
	defer decoder.Close()

	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append([]byte{0, 'x'})
	refs := builder.NewBinaryArray()
	defer refs.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logical, err := decoder.Decode(ctx, refs)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, logical)
}
