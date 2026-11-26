// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importutilv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestSortFieldIDsDerivesFromSchema(t *testing.T) {
	pkOnly := []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}
	withPartitionKey := append(pkOnly, &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_VarChar, IsPartitionKey: true})
	for _, tc := range []struct {
		name      string
		namespace bool
		mode      string
		fields    []*schemapb.FieldSchema
		want      []int64
	}{
		{"ordinary", false, "", pkOnly, []int64{100}},
		{"ordinary with partition key", false, "", withPartitionKey, []int64{100}},
		{"partition namespace", true, common.NamespaceModePartition, pkOnly, []int64{100}},
		{"partition key namespace", true, common.NamespaceModePartitionKey, withPartitionKey, []int64{101, 100}},
		{"legacy namespace", true, "", withPartitionKey, []int64{101, 100}},
		{"namespace missing partition key", true, common.NamespaceModePartitionKey, pkOnly, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{EnableNamespace: tc.namespace, Fields: tc.fields}
			if tc.mode != "" {
				schema.Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: tc.mode}}
			}
			ids, err := SortFieldIDs(schema)
			if tc.want == nil {
				require.ErrorContains(t, err, "partition key field is not found")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, ids)
		})
	}
}

func TestFragmentSchemaDerivesFromCollectionSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text},
	}}

	// Ordinary fragments materialize RowID exactly once, never a second copy of
	// a system field the collection schema already carries.
	ordinary := FragmentSchema(schema, false)
	require.Equal(t, []int64{100, 101, int64(common.RowIDField)}, fieldIDs(ordinary))

	// Backup fragments keep the full system field set.
	backup := FragmentSchema(schema, true)
	require.Equal(t, []int64{100, 101, int64(common.RowIDField), int64(common.TimeStampField)}, fieldIDs(backup))

	// TEXT is mapped to VarChar with a placeholder max_length.
	text := ordinary.GetFields()[1]
	require.Equal(t, schemapb.DataType_VarChar, text.GetDataType())
	require.Len(t, text.GetTypeParams(), 1)
	require.Equal(t, common.MaxLengthKey, text.GetTypeParams()[0].GetKey())
}

func fieldIDs(schema *schemapb.CollectionSchema) []int64 {
	ids := make([]int64, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		ids = append(ids, field.GetFieldID())
	}
	return ids
}
