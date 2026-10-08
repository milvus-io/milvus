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

package dql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestConvertPlaceholderGroupSparseValidation(t *testing.T) {
	// One cell: index 1, value 1.0.
	valid := []byte{1, 0, 0, 0, 0, 0, 128, 63}
	sparse := func(rows ...[]byte) *commonpb.PlaceholderValue {
		return &commonpb.PlaceholderValue{Tag: "$0", Type: commonpb.PlaceholderType_SparseFloatVector, Values: rows}
	}
	text := &commonpb.PlaceholderValue{Tag: "$0", Type: commonpb.PlaceholderType_VarChar, Values: [][]byte{[]byte("query")}}
	for _, tc := range []struct {
		name         string
		placeholders []*commonpb.PlaceholderValue
		invalid      bool
	}{
		{"valid rows", []*commonpb.PlaceholderValue{sparse(valid, valid)}, false},
		{"empty row", []*commonpb.PlaceholderValue{sparse([]byte{})}, false},
		{"BM25 text", []*commonpb.PlaceholderValue{text}, false},
		{"short row", []*commonpb.PlaceholderValue{sparse(make([]byte, 1))}, true},
		{"9 bytes", []*commonpb.PlaceholderValue{sparse(make([]byte, 9))}, true},
		{"17 bytes", []*commonpb.PlaceholderValue{sparse(make([]byte, 17))}, true},
		{"25 bytes", []*commonpb.PlaceholderValue{sparse(make([]byte, 25))}, true},
		{"later row", []*commonpb.PlaceholderValue{sparse(valid, make([]byte, 9))}, true},
		{"later placeholder", []*commonpb.PlaceholderValue{sparse(valid), sparse(make([]byte, 9))}, true},
		{"after text placeholder", []*commonpb.PlaceholderValue{text, sparse(make([]byte, 9))}, true},
		{"duplicate indices", []*commonpb.PlaceholderValue{sparse(append(append([]byte{}, valid...), valid...))}, true},
		{"NaN", []*commonpb.PlaceholderValue{sparse([]byte{1, 0, 0, 0, 0, 0, 192, 127})}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: tc.placeholders})
			require.NoError(t, err)
			converted, _, err := ConvertPlaceholderGroup(blob, &schemapb.FieldSchema{DataType: schemapb.DataType_SparseFloatVector})
			if tc.invalid {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Equal(t, int32(1100), merr.Status(err).GetCode())
				require.False(t, merr.Status(err).GetRetriable())
				require.Equal(t, "true", merr.Status(err).GetExtraInfo()[merr.InputErrorFlagKey])
				return
			}
			require.NoError(t, err)
			require.Equal(t, blob, converted)
		})
	}
}

func TestSearchTaskSparsePlaceholderValidation(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "sparse_validation",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	})
	params := []*commonpb.KeyValuePair{
		{Key: AnnsFieldKey, Value: "sparse"},
		{Key: TopKKey, Value: "10"},
		{Key: "metric_type", Value: "IP"},
	}
	for _, hybrid := range []bool{false, true} {
		for _, size := range []int{8, 9, 17, 25} {
			t.Run(fmt.Sprintf("hybrid=%t/size=%d", hybrid, size), func(t *testing.T) {
				blob, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
					Tag: "$0", Type: commonpb.PlaceholderType_SparseFloatVector,
					Values: [][]byte{{1, 0, 0, 0, 0, 0, 128, 63}, make([]byte, size)},
				}}})
				require.NoError(t, err)
				task := &SearchTask{
					ctx: ctx, collectionName: "sparse_validation", schema: schema,
					SearchRequest: &internalpb.SearchRequest{CollectionID: 1},
					request: &milvuspb.SearchRequest{
						CollectionName: "sparse_validation", Nq: 2, SearchParams: params,
						SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: blob},
					},
					tr: timerecord.NewTimeRecorder("sparse-validation"),
				}
				if hybrid {
					validBlob, marshalErr := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
						Tag: "$0", Type: commonpb.PlaceholderType_SparseFloatVector,
						Values: [][]byte{make([]byte, 8), make([]byte, 8)},
					}}})
					require.NoError(t, marshalErr)
					task.request.SubReqs = []*milvuspb.SubSearchRequest{
						{PlaceholderGroup: validBlob, Nq: 2, SearchParams: params},
						{PlaceholderGroup: blob, Nq: 2, SearchParams: params},
					}
					task.request.SearchParams = []*commonpb.KeyValuePair{{Key: LimitKey, Value: "10"}}
					err = task.initAdvancedSearchRequest(ctx)
				} else {
					err = task.initSearchRequest(ctx)
				}
				if size == 8 {
					require.NoError(t, err)
					if hybrid {
						require.Equal(t, blob, task.SubReqs[1].GetPlaceholderGroup())
					} else {
						require.Equal(t, blob, task.PlaceholderGroup)
					}
					return
				}
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Contains(t, err.Error(), "8-byte aligned")
				require.Equal(t, int32(1100), merr.Status(err).GetCode())
			})
		}
	}
}
