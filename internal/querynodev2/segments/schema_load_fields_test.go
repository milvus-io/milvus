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

func TestSchemaUpdateAddsLoadFieldsWithoutExpandingOldSelection(t *testing.T) {
	paramtable.Init()
	for _, viaLoad := range []bool{false, true} {
		name := "schema_rpc"
		if viaLoad {
			name = "segment_load"
		}
		t.Run(name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("load-hints", schemapb.DataType_Int64, false)
			manager := NewCollectionManager()
			require.NoError(t, manager.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{LoadFields: []int64{100, 101}}))
			collection := manager.Get(10)
			defer DeleteCollection(collection)
			apply := func(next *schemapb.CollectionSchema, version uint64) (uint64, bool, error) {
				before := collection.schemaVersion
				var err error
				if viaLoad {
					refs := collection.refCount.Load()
					err = manager.PutOrRef(10, next, nil, &querypb.LoadMetaInfo{SchemaVersion: version})
					if err != nil {
						require.Equal(t, refs, collection.refCount.Load(), "failed load must not acquire a reference")
					}
				} else {
					err = manager.UpdateSchema(10, next, version)
				}
				return collection.schemaVersion, collection.schemaVersion != before, err
			}
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
			_, changed, err := apply(next, 100)
			require.ErrorIs(t, err, failed)
			require.False(t, changed)
			require.Equal(t, typeutil.NewSet[int64](100, 101), collection.loadFields)
			require.Same(t, schema, collection.Schema(), "failed hint application must not publish the Go schema")
			_, changed, err = apply(next, 100)
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, typeutil.NewSet[int64](100, 101, 200), collection.loadFields)
			require.Equal(t, 2, calls)
			_, changed, err = apply(next, 100)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, 2, calls, "same schema replay does not rewrite hints")
		})
	}
}

func TestSchemaUpdateDoesNotReuseNativeVersionAfterHintFailure(t *testing.T) {
	paramtable.Init()
	for _, viaLoad := range []bool{false, true} {
		name := "schema_rpc"
		if viaLoad {
			name = "segment_load"
		}
		t.Run(name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("load-hint-versions", schemapb.DataType_Int64, false)
			manager := NewCollectionManager()
			require.NoError(t, manager.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{LoadFields: []int64{100, 101}}))
			collection := manager.Get(10)
			defer DeleteCollection(collection)
			apply := func(next *schemapb.CollectionSchema, version uint64) (uint64, bool, error) {
				before := collection.schemaVersion
				var err error
				if viaLoad {
					refs := collection.refCount.Load()
					err = manager.PutOrRef(10, next, nil, &querypb.LoadMetaInfo{SchemaVersion: version})
					if err != nil {
						require.Equal(t, refs, collection.refCount.Load(), "failed load must not acquire a reference")
					}
				} else {
					err = manager.UpdateSchema(10, next, version)
				}
				return collection.schemaVersion, collection.schemaVersion != before, err
			}
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
				if hintCalls <= 2 {
					return failed
				}
				return nativeHints(c, fields)
			}).Build()
			defer hintPatch.UnPatch()

			_, changed, err := apply(first, 100)
			require.ErrorIs(t, err, failed)
			require.False(t, changed)
			require.Same(t, schema, collection.Schema())
			_, changed, err = apply(first, 100)
			require.ErrorIs(t, err, failed)
			require.False(t, changed)
			require.Same(t, schema, collection.Schema())
			// A newer payload arrives before the failed one is retried. It must get a
			// fresh native version instead of being silently skipped by segcore.
			_, changed, err = apply(second, 101)
			require.NoError(t, err)
			require.True(t, changed)
			require.Len(t, versions, 3)
			require.Greater(t, versions[1], versions[0])
			require.Greater(t, versions[2], versions[1])
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
			plan, err := segcore.NewRetrievePlan(collection.ccollection, expr, 0, 0, commonpb.ConsistencyLevel_Strong, 0)
			require.NoError(t, err)
			defer plan.Delete()
			_, changed, err = apply(first, 100)
			require.NoError(t, err)
			require.False(t, changed, "the superseded failed payload cannot overwrite the newer schema")
			require.Len(t, versions, 3)
		})
	}
}

func TestSchemaUpdateFailureDoesNotPublishOrAcquireReference(t *testing.T) {
	paramtable.Init()
	for _, viaLoad := range []bool{false, true} {
		name := "schema_rpc"
		if viaLoad {
			name = "segment_load"
		}
		t.Run(name, func(t *testing.T) {
			schema := mock_segcore.GenTestCollectionSchema("failed-schema", schemapb.DataType_Int64, false)
			manager := NewCollectionManager()
			require.NoError(t, manager.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{LoadFields: []int64{100, 101}}))
			collection := manager.Get(10)
			defer DeleteCollection(collection)
			next := proto.Clone(schema).(*schemapb.CollectionSchema)
			next.Version++
			next.Fields = append(next.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "new_field", DataType: schemapb.DataType_Int64, Nullable: true})
			failed := merr.WrapErrServiceUnavailable("injected native schema failure")
			patch := mockey.Mock((*segcore.CCollection).UpdateSchema).Return(failed).Build()
			defer patch.UnPatch()
			var err error
			if viaLoad {
				err = manager.PutOrRef(10, next, nil, &querypb.LoadMetaInfo{SchemaVersion: 100})
			} else {
				err = manager.UpdateSchema(10, next, 100)
			}
			require.ErrorIs(t, err, failed)
			require.Same(t, schema, collection.Schema())
			require.Zero(t, collection.schemaVersion)
			require.EqualValues(t, 1, collection.refCount.Load())
			require.Equal(t, typeutil.NewSet[int64](100, 101), collection.loadFields)
			patch.UnPatch()
			require.NoError(t, manager.UpdateSchema(10, next, 100))
			require.Same(t, next, collection.Schema())
		})
	}
}
