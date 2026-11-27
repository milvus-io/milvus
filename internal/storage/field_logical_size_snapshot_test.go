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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestFieldLogicalSizesCoverManifest(t *testing.T) {
	sizes := &datapb.FieldLogicalSizes{Version: FieldLogicalSizeVersion, RowCount: 100}
	path := packed.MarshalManifestPath("test/segment", 2)
	require.True(t, FieldLogicalSizesCoverManifest(sizes, 100, path, path))
	require.True(t, FieldLogicalSizesCoverManifest(sizes, 100, `{"ver":2,"base_path":"test/segment"}`, path))
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 99, path, path))
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 100, "", path))
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 100, packed.MarshalManifestPath("test/segment", 1), path))
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 100, packed.MarshalManifestPath("test/other-segment", 2), path))
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 100, "invalid", "invalid"))
	emptyManifest := packed.MarshalManifestPath("test/segment", packed.ManifestEarliest)
	require.False(t, FieldLogicalSizesCoverManifest(sizes, 100, emptyManifest, emptyManifest))
}

func TestAppendFieldLogicalSizes(t *testing.T) {
	prefix := &datapb.FieldLogicalSizes{Version: 1, RowCount: 100, Fields: map[int64]*datapb.FieldLogicalSize{
		101: {PayloadBytes: 300, ValidityBits: 100}, 102: {}, 103: {PayloadBytes: 12},
		104: {PayloadBytes: math.MaxUint64}, 105: {ValidityBits: math.MaxUint64},
	}}
	batch := &datapb.FieldLogicalSizes{Version: 1, RowCount: 20, Fields: map[int64]*datapb.FieldLogicalSize{
		101: {PayloadBytes: 60, ValidityBits: 20}, 102: {}, 104: {PayloadBytes: 1}, 105: {ValidityBits: 1}, 106: {},
	}}
	joined := AppendFieldLogicalSizes(prefix, 100, batch, 20)
	require.True(t, FieldLogicalSizesCover(joined, 120))
	require.Len(t, joined.Fields, 2)
	require.Equal(t, uint64(360), joined.Fields[101].PayloadBytes)
	require.Equal(t, uint64(120), joined.Fields[101].ValidityBits)
	require.NotNil(t, joined.Fields[102])

	// A zero-length protobuf message in the map represents a measured zero on the wire.
	wire, err := proto.Marshal(&datapb.Statistics{FieldLogicalSizes: joined})
	require.NoError(t, err)
	var restored datapb.Statistics
	require.NoError(t, proto.Unmarshal(wire, &restored))
	require.True(t, proto.Equal(joined, restored.FieldLogicalSizes))
	require.NotContains(t, restored.FieldLogicalSizes.Fields, int64(103))

	require.Nil(t, AppendFieldLogicalSizes(nil, 100, batch, 20))
	require.Nil(t, AppendFieldLogicalSizes(prefix, 100, nil, 20))
	require.Nil(t, AppendFieldLogicalSizes(prefix, 99, batch, 20))
	require.Nil(t, AppendFieldLogicalSizes(prefix, 100, batch, 19))
	require.Nil(t, AppendFieldLogicalSizes(prefix, math.MaxInt64, batch, 20))
	future := proto.CloneOf(prefix)
	future.Version++
	require.Nil(t, AppendFieldLogicalSizes(future, 100, batch, 20))

	first := AppendFieldLogicalSizes(nil, 0, batch, 20)
	require.True(t, proto.Equal(batch, first))
	emptyFlush := AppendFieldLogicalSizes(joined, 120, nil, 0)
	require.True(t, proto.Equal(joined, emptyFlush))
	first.Fields[101].PayloadBytes = 0
	emptyFlush.Fields[101].PayloadBytes = 0
	require.Equal(t, uint64(60), batch.Fields[101].PayloadBytes)
	require.Equal(t, uint64(360), joined.Fields[101].PayloadBytes)
}
