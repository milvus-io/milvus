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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func addedFieldSchema(previous *schemapb.CollectionSchema, id int64, name string) *schemapb.CollectionSchema {
	next := proto.Clone(previous).(*schemapb.CollectionSchema)
	next.Version++
	next.Fields = append(next.Fields, &schemapb.FieldSchema{
		FieldID: id, Name: name, DataType: schemapb.DataType_Int64, Nullable: true,
	})
	return next
}

func requireNativeField(t *testing.T, collection *Collection, fieldID int64) {
	t.Helper()
	// Verify the real native schema: Go metadata alone cannot prove publication.
	predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: fieldID, DataType: schemapb.DataType_Int64},
		Op:         planpb.OpType_GreaterThan,
		Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
	}}}
	expr, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: predicate}})
	require.NoError(t, err)
	plan, err := segcore.NewRetrievePlan(collection.ccollection, expr, 0, 0, commonpb.ConsistencyLevel_Strong, 0)
	require.NoError(t, err)
	plan.Delete()
}

func testSchemaUpdateEntryPoints(t *testing.T, test func(*testing.T, *Collection, func(*schemapb.CollectionSchema, uint64) error)) {
	t.Helper()
	paramtable.Init()
	for _, viaLoad := range []bool{false, true} {
		name := "schema_rpc"
		if viaLoad {
			name = "segment_load"
		}
		t.Run(name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("load-hints", schemapb.DataType_Int64, false)
			schema.Version = 1
			manager := NewCollectionManager()
			require.NoError(t, manager.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
				LoadFields: []int64{100, 101}, SchemaVersion: 1000,
			}))
			collection := manager.Get(10)
			defer DeleteCollection(collection)
			apply := func(next *schemapb.CollectionSchema, timestamp uint64) error {
				if !viaLoad {
					return manager.UpdateSchema(10, next, timestamp)
				}
				refs := collection.refCount.Load()
				err := manager.PutOrRef(10, next, nil, &querypb.LoadMetaInfo{SchemaVersion: timestamp})
				if err != nil {
					require.Equal(t, refs, collection.refCount.Load(), "failed load must not acquire a reference")
				}
				return err
			}
			test(t, collection, apply)
		})
	}
}

func TestSchemaUpdateAddsLoadFieldsWithoutExpandingOldSelection(t *testing.T) {
	testSchemaUpdateEntryPoints(t, func(t *testing.T, collection *Collection, apply func(*schemapb.CollectionSchema, uint64) error) {
		schema := collection.Schema()
		next := addedFieldSchema(schema, 200, "added")
		next.Fields = append(next.Fields, &schemapb.FieldSchema{
			FieldID: 201, Name: "cold", DataType: schemapb.DataType_Int64, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}},
		})
		var nativeUpdate func(*segcore.CCollection, *schemapb.CollectionSchema, []int64) error
		calls := 0
		failed := merr.WrapErrServiceUnavailable("injected native schema and hint failure")
		patch := mockey.Mock((*segcore.CCollection).UpdateSchema).Origin(&nativeUpdate).To(func(c *segcore.CCollection, sch *schemapb.CollectionSchema, fields []int64) error {
			calls++
			require.ElementsMatch(t, []int64{100, 101, 200}, fields)
			if calls == 1 {
				return failed
			}
			return nativeUpdate(c, sch, fields)
		}).Build()
		defer patch.UnPatch()

		require.ErrorIs(t, apply(next, 100), failed)
		require.Same(t, schema, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101), collection.loadFields)
		require.EqualValues(t, 1, collection.refCount.Load())

		// Retrying the exact schema and timestamp must apply both native parts.
		require.NoError(t, apply(next, 100))
		require.Same(t, next, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101, 200), collection.loadFields)
		requireNativeField(t, collection, 200)
		require.NoError(t, apply(next, 5000))
		require.Equal(t, 2, calls, "same schema with a higher timetick must not rewrite hints")
	})
}

func TestSchemaUpdateOrdersBySchemaVersionAcrossChannels(t *testing.T) {
	testSchemaUpdateEntryPoints(t, func(t *testing.T, collection *Collection, apply func(*schemapb.CollectionSchema, uint64) error) {
		first := collection.Schema()
		second := addedFieldSchema(first, 200, "second")
		// A2's per-channel tick can be below A1's max tick used at creation.
		require.NoError(t, apply(second, 100))
		require.Same(t, second, collection.Schema())
		requireNativeField(t, collection, 200)

		// A delayed older schema can carry a larger tick from another channel.
		require.NoError(t, apply(first, 10000))
		require.Same(t, second, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101, 200), collection.loadFields)
		requireNativeField(t, collection, 200)

		third := addedFieldSchema(second, 201, "third")
		require.NoError(t, apply(third, 50))
		require.Same(t, third, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101, 200, 201), collection.loadFields)
		requireNativeField(t, collection, 201)
	})
}

func TestSchemaUpdateFailureDoesNotAllowStaleSchemaToOverwrite(t *testing.T) {
	testSchemaUpdateEntryPoints(t, func(t *testing.T, collection *Collection, apply func(*schemapb.CollectionSchema, uint64) error) {
		first := collection.Schema()
		second := addedFieldSchema(first, 200, "second")
		third := addedFieldSchema(second, 201, "third")
		failed := merr.WrapErrServiceUnavailable("injected native update failure")
		patch := mockey.Mock((*segcore.CCollection).UpdateSchema).Return(failed).Build()
		defer patch.UnPatch()
		require.ErrorIs(t, apply(second, 100), failed)
		require.Same(t, first, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101), collection.loadFields)

		// A replay after failure must remain a no-op even with a larger tick.
		require.NoError(t, apply(first, 10000))
		require.Same(t, first, collection.Schema())
		patch.UnPatch()
		// A later schema can supersede the failed attempt without retrying it.
		require.NoError(t, apply(third, 50))
		require.NoError(t, apply(second, 20000))
		require.Same(t, third, collection.Schema())
		require.Equal(t, typeutil.NewSet[int64](100, 101, 200, 201), collection.loadFields)
		requireNativeField(t, collection, 201)
	})
}
