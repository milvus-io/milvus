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

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestApplyCollectionMmapSetting(t *testing.T) {
	mmap := func(value string) []*commonpb.KeyValuePair {
		if value == "" {
			return nil
		}
		return []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: value}}
	}
	for _, test := range []struct {
		name, collection, structure, inherited string
	}{
		{name: "unset"},
		{name: "collection enabled", collection: "true", inherited: "true"},
		{name: "collection disabled", collection: "false", inherited: "false"},
		{name: "struct enabled", structure: "true", inherited: "true"},
		{name: "struct disabled", structure: "false", inherited: "false"},
		{name: "struct overrides enabled collection", collection: "true", structure: "false", inherited: "false"},
		{name: "struct overrides disabled collection", collection: "false", structure: "true", inherited: "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				Name: "collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
					{FieldID: 101, TypeParams: mmap("true")},
					{FieldID: 102, TypeParams: mmap("false")},
				},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{{
					FieldID: 200, TypeParams: mmap(test.structure),
					Fields: []*schemapb.FieldSchema{
						{FieldID: 201},
						{FieldID: 202, TypeParams: mmap("true")},
						{FieldID: 203, TypeParams: mmap("false")},
					},
				}},
			}
			original := proto.Clone(schema).(*schemapb.CollectionSchema)
			owned := proto.Clone(schema).(*schemapb.CollectionSchema)
			result := ApplyCollectionMmapSetting(owned, mmap(test.collection))
			require.Same(t, owned, result)
			require.True(t, proto.Equal(original, schema))
			require.Equal(t, append([]*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}, mmap(test.collection)...), result.Fields[0].TypeParams)
			require.Equal(t, mmap("true"), result.Fields[1].TypeParams)
			require.Equal(t, mmap("false"), result.Fields[2].TypeParams)
			structure := result.StructArrayFields[0]
			require.Equal(t, mmap(test.inherited), structure.TypeParams)
			require.Equal(t, mmap(test.inherited), structure.Fields[0].TypeParams)
			require.Equal(t, mmap("true"), structure.Fields[1].TypeParams)
			require.Equal(t, mmap("false"), structure.Fields[2].TypeParams)
			applied := proto.Clone(result)
			ApplyCollectionMmapSetting(result, mmap(test.collection))
			require.True(t, proto.Equal(applied, result), "reapplying defaults must not duplicate parameters")
		})
	}
	t.Run("empty schema", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{}
		require.Same(t, schema, ApplyCollectionMmapSetting(schema, mmap("true")))
	})
}
