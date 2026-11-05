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

package cmek

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestRequestedFieldIDsUsesAcceptedSchema(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields:             []*schemapb.FieldSchema{{Name: "pk", FieldID: 105}, {Name: "$meta", FieldID: 109, IsDynamic: true}},
		EnableDynamicField: true,
		StructArrayFields:  []*schemapb.StructArrayFieldSchema{{Name: "items", FieldID: 111, Fields: []*schemapb.FieldSchema{{Name: "value", FieldID: 113}, {Name: "vector", FieldID: 117}}}},
	}
	original := proto.Clone(schema)
	data := []*schemapb.FieldData{
		{FieldName: "pk"},
		{FieldName: "$meta", IsDynamic: true},
		{FieldName: "items", Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
			Fields: []*schemapb.FieldData{{FieldName: "value"}, {FieldName: "vector"}},
		}}},
	}
	s := new(rawDataSuite)
	s.SetT(t)
	s.bindRawDataFieldIDs(schema, data)
	require.Equal(t, int64(105), data[0].GetFieldId())
	require.Equal(t, int64(109), data[1].GetFieldId())
	require.Equal(t, int64(111), data[2].GetFieldId())
	require.Equal(t, int64(113), data[2].GetStructArrays().GetFields()[0].GetFieldId())
	require.Equal(t, int64(117), data[2].GetStructArrays().GetFields()[1].GetFieldId())
	require.Equal(t, []int64{105, 113, 117, 109}, requestedFieldIDs(schema, []string{"pk", "items", "$meta"}))
	require.True(t, proto.Equal(original, schema), "resolving load fields must not renumber the accepted schema")
}
