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

package segcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestComposeCollectionIndexMetaRetiredJSONFlat(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_JSON},
		{FieldID: 101, DataType: schemapb.DataType_Int64},
		{FieldID: 102, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}
	infos := []*indexpb.IndexInfo{
		{IndexID: 1, FieldID: 100, IndexParams: []*commonpb.KeyValuePair{{Key: "json_cast_type", Value: "JSON"}}},
		{IndexID: 2, FieldID: 100, IndexParams: []*commonpb.KeyValuePair{{Key: "json_cast_type", Value: "DOUBLE"}}},
		{IndexID: 3, FieldID: 101, IndexParams: []*commonpb.KeyValuePair{{Key: "json_cast_type", Value: "JSON"}}},
		{IndexID: 4, FieldID: 102, IndexParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "FLAT"}}},
	}
	before := proto.Clone(infos[0])
	physical := PhysicalIndexInfos(infos, schema)
	require.Len(t, physical, 3)
	require.EqualValues(t, 2, physical[0].IndexID)
	native := ComposeCollectionIndexMeta(context.Background(), infos, schema)
	require.Len(t, native.IndexMetas, 3)
	for i, id := range []int64{2, 3, 4} {
		require.Equal(t, id, native.IndexMetas[i].IndexId)
	}
	require.Len(t, infos, 4)
	require.True(t, proto.Equal(before, infos[0]))
	physical[0] = nil
	require.NotNil(t, infos[1], "physical projection cannot mutate the shared metadata slice")
}
