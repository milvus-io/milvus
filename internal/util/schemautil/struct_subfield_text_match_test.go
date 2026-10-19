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

package schemautil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRejectStructSubFieldsTextMatch(t *testing.T) {
	newStruct := func(field *schemapb.FieldSchema) []*schemapb.StructArrayFieldSchema {
		return []*schemapb.StructArrayFieldSchema{{Name: "s", Fields: []*schemapb.FieldSchema{field}}}
	}
	varcharArray := func(params ...*commonpb.KeyValuePair) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name: "s[tag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar,
			TypeParams: append([]*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "4"},
				{Key: common.MaxLengthKey, Value: "16"},
			}, params...),
		}
	}

	for _, key := range []string{common.EnableMatchKey, common.EnableAnalyzerKey} {
		err := RejectStructSubFieldsTextMatch(newStruct(varcharArray(&commonpb.KeyValuePair{Key: key, Value: "true"})))
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, key+" is not supported on struct sub-field s[tag]")
	}
	require.NoError(t, RejectStructSubFieldsTextMatch(newStruct(varcharArray(&commonpb.KeyValuePair{Key: common.EnableMatchKey, Value: "false"}))))

	nested := &schemapb.FieldSchema{
		Name: "s[tags]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "8"}},
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxLengthKey, Value: "16"},
						{Key: common.EnableAnalyzerKey, Value: "true"},
					},
					Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_VarChar},
				}},
			}},
		},
	}
	require.ErrorContains(t, RejectStructSubFieldsTextMatch(newStruct(nested)), "enable_analyzer is not supported on struct sub-field s[tags]")
}
