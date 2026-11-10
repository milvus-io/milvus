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

package segments

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSchemaUpdateAddsLoadFieldsWithoutExpandingOldSelection(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("load-hints", schemapb.DataType_Int64, false)
	collection, err := NewCollection(10, schema, nil, &querypb.LoadMetaInfo{LoadFields: []int64{100, 101}})
	require.NoError(t, err)
	defer DeleteCollection(collection)
	next := proto.Clone(schema).(*schemapb.CollectionSchema)
	next.Version = schema.GetVersion() + 1
	next.Fields = append(next.Fields,
		&schemapb.FieldSchema{FieldID: 200, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true},
		&schemapb.FieldSchema{FieldID: 201, Name: "cold", DataType: schemapb.DataType_Int64, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}})
	var nativeUpdate func(*segcore.CCollection, []int64) error
	calls := 0
	failed := merr.WrapErrServiceUnavailable("injected native hint failure")
	patch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).Origin(&nativeUpdate).To(func(c *segcore.CCollection, fields []int64) error {
		calls++
		require.ElementsMatch(t, []int64{100, 101, 200}, fields)
		if calls == 1 {
			return failed
		}
		return nativeUpdate(c, fields)
	}).Build()
	defer patch.UnPatch()
	_, changed, err := collection.applySchemaUpdate(next, uint64(next.Version), 100)
	require.ErrorIs(t, err, failed)
	require.False(t, changed)
	require.Equal(t, typeutil.NewSet[int64](100, 101), collection.loadFields)
	require.Same(t, schema, collection.Schema(), "failed hint application must not publish the Go schema")
	_, changed, err = collection.applySchemaUpdate(next, uint64(next.Version), 100)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, typeutil.NewSet[int64](100, 101, 200), collection.loadFields)
	require.Equal(t, 2, calls)
	_, changed, err = collection.applySchemaUpdate(next, uint64(next.Version), 100)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, 2, calls, "same schema replay does not rewrite hints")
}

func TestSchemaUpdateDoesNotReuseNativeVersionAfterHintFailure(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("load-hint-versions", schemapb.DataType_Int64, false)
	collection, err := NewCollection(10, schema, nil, &querypb.LoadMetaInfo{LoadFields: []int64{100, 101}})
	require.NoError(t, err)
	defer DeleteCollection(collection)
	first := proto.Clone(schema).(*schemapb.CollectionSchema)
	first.Version++
	first.Fields = append(first.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "first", DataType: schemapb.DataType_Int64, Nullable: true})
	second := proto.Clone(first).(*schemapb.CollectionSchema)
	second.Version++
	second.Fields = append(second.Fields, &schemapb.FieldSchema{FieldID: 201, Name: "second", DataType: schemapb.DataType_Int64, Nullable: true})

	var nativeUpdate func(*segcore.CCollection, *schemapb.CollectionSchema, uint64) error
	var versions []uint64
	versionPatch := mockey.Mock((*segcore.CCollection).UpdateSchema).Origin(&nativeUpdate).To(func(c *segcore.CCollection, next *schemapb.CollectionSchema, version uint64) error {
		versions = append(versions, version)
		return nativeUpdate(c, next, version)
	}).Build()
	defer versionPatch.UnPatch()
	var nativeHints func(*segcore.CCollection, []int64) error
	failed := merr.WrapErrServiceUnavailable("injected native hint failure")
	hintCalls := 0
	hintPatch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).Origin(&nativeHints).To(func(c *segcore.CCollection, fields []int64) error {
		hintCalls++
		if hintCalls == 1 {
			return failed
		}
		return nativeHints(c, fields)
	}).Build()
	defer hintPatch.UnPatch()

	_, changed, err := collection.applySchemaUpdate(first, uint64(first.Version), 100)
	require.ErrorIs(t, err, failed)
	require.False(t, changed)
	require.Same(t, schema, collection.Schema())
	// A newer payload arrives before the failed one is retried. It must get a
	// fresh native version instead of being silently skipped by segcore.
	_, changed, err = collection.applySchemaUpdate(second, uint64(second.Version), 200)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, versions, 2)
	require.Greater(t, versions[1], versions[0])
	require.Same(t, second, collection.Schema())
	require.Equal(t, typeutil.NewSet[int64](100, 101, 200, 201), collection.loadFields)

	// Check the real native schema, not only the outer Go snapshot: compiling
	// a predicate on the second added field fails if segcore retained first.
	predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 201, DataType: schemapb.DataType_Int64},
		Op:         planpb.OpType_GreaterThan,
		Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
	}}}
	expr, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: predicate}})
	require.NoError(t, err)
	plan, err := segcore.NewRetrievePlan(collection.ccollection, expr, 0, 0, commonpb.ConsistencyLevel_Strong, 0, 0)
	require.NoError(t, err)
	defer plan.Delete()
	_, changed, err = collection.applySchemaUpdate(first, uint64(first.Version), 100)
	require.NoError(t, err)
	require.False(t, changed, "the superseded failed payload cannot overwrite the newer schema")
	require.Len(t, versions, 2)
}
