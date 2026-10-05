// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package segcore_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the real C ABI rather than inferring execution from the Go request.
// The shared fixture inserts ten rows in memory and uses no remote storage.
func TestIteratorPKCursorNativeGetters(t *testing.T) {
	collection, segment, ordinary := newBoostScoreTestContext(t)
	defer collection.Release()
	defer segment.Release()
	defer ordinary.Delete()

	check := func(t *testing.T, request *segcore.SearchRequest, version uint32, executed bool) {
		t.Helper()
		require.Equal(t, version, request.Plan().IteratorPKCursorVersion())
		result, err := segment.Search(context.Background(), request)
		require.NoError(t, err)
		require.NotNil(t, result)
		defer result.Release()
		require.Equal(t, executed, result.IteratorPKCursorExecuted())
	}
	t.Run("ordinary search", func(t *testing.T) { check(t, ordinary, 0, false) })
	for _, version := range []uint32{0, 2} {
		t.Run("iterator version "+strconv.Itoa(int(version)), func(t *testing.T) {
			request := newNativeCursorSearchRequest(t, collection, version)
			defer request.Delete()
			check(t, request, version, version == 2)
		})
	}
}

func newNativeCursorSearchRequest(t *testing.T, collection *segcore.CCollection, version uint32) *segcore.SearchRequest {
	t.Helper()
	var vectorField *schemapb.FieldSchema
	for _, field := range collection.Schema().Fields {
		if field.DataType == schemapb.DataType_FloatVector {
			vectorField = field
			break
		}
	}
	require.NotNil(t, vectorField)
	var dim int
	for _, parameter := range vectorField.TypeParams {
		if parameter.Key == "dim" {
			var err error
			dim, err = strconv.Atoi(parameter.Value)
			require.NoError(t, err)
		}
	}
	require.Positive(t, dim)
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
		VectorType: planpb.VectorType_FloatVector, FieldId: vectorField.FieldID, PlaceholderTag: "$0",
		QueryInfo: &planpb.QueryInfo{
			Topk: 2, MetricType: ordinaryMetric(vectorField), RoundDecimal: -1, SearchParams: "{}",
			SearchIteratorV2Info: &planpb.SearchIteratorV2Info{Token: "9c38d167-1d2a-44b5-839d-39b58b5944b2", BatchSize: 2, CursorVersion: version},
		},
	}}})
	require.NoError(t, err)
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{
		{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, dim*4)}},
	}})
	require.NoError(t, err)
	request, err := segcore.NewSearchRequest(collection, &querypb.SearchRequest{Req: &internalpb.SearchRequest{
		CollectionID: collection.ID(), Nq: 1, SerializedExprPlan: plan, MvccTimestamp: typeutil.MaxTimestamp,
	}}, placeholder)
	require.NoError(t, err)
	return request
}

func ordinaryMetric(field *schemapb.FieldSchema) string {
	for _, parameter := range field.IndexParams {
		if parameter.Key == "metric_type" {
			return parameter.Value
		}
	}
	return "L2"
}
