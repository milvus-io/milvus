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

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const arrayFeatureGateMessage = "element_nullable and nested Array fields are not supported yet"

// setArrayFeatureGate turns the gate on (switch off) or off (switch on) for one
// test. TestMain turns the switch on for the rest of the package, so the
// cleanup restores that.
func setArrayFeatureGate(t *testing.T, gated bool) {
	key := paramtable.Get().ProxyCfg.EnableElementNullAndNestedArray.Key
	value := "true"
	if gated {
		value = "false"
	}
	paramtable.Get().Save(key, value)
	t.Cleanup(func() { paramtable.Get().Save(key, "true") })
}

// requireGateRejects checks that an otherwise valid request is refused as an
// input error. The rejection may come from the gate or from an older check of
// the same shape that runs first; both say the feature is not supported.
func requireGateRejects(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "not supported")
}

// requireNotGated checks that the gate did not refuse the request. Other
// validation may still reject the shape on this version.
func requireNotGated(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		assert.NotContains(t, err.Error(), arrayFeatureGateMessage)
	}
}

type gatedSubField struct {
	name  string
	field func() *schemapb.FieldSchema
}

func gatedSubFields() []gatedSubField {
	return []gatedSubField{
		{"nested array", func() *schemapb.FieldSchema {
			return &schemapb.FieldSchema{
				Name:        "nested",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Array,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
				TypeSchema: &schemapb.TypeSchema{
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
					Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
						TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
						Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64},
						}},
					}},
				},
			}
		}},
		{"element-nullable scalar array", func() *schemapb.FieldSchema {
			return &schemapb.FieldSchema{
				Name:            "ints",
				DataType:        schemapb.DataType_Array,
				ElementType:     schemapb.DataType_Int64,
				ElementNullable: true,
				TypeParams:      []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
			}
		}},
		{"element-nullable vector array", func() *schemapb.FieldSchema {
			return &schemapb.FieldSchema{
				Name:            "vectors",
				DataType:        schemapb.DataType_ArrayOfVector,
				ElementType:     schemapb.DataType_FloatVector,
				ElementNullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "8"},
					{Key: common.MaxCapacityKey, Value: "16"},
				},
			}
		}},
	}
}

func gateTestSchema(subField *schemapb.FieldSchema) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name: "gate_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
			},
		},
	}
	if subField != nil {
		schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{
			FieldID: 102,
			Name:    "s",
			Fields:  []*schemapb.FieldSchema{subField},
		}}
	}
	return schema
}

func TestArrayFeatureGate_SwitchOnForPackageTests(t *testing.T) {
	assert.True(t, paramtable.Get().ProxyCfg.EnableElementNullAndNestedArray.GetAsBool())
	assert.False(t, arrayFeatureGateEnabled())
}

func TestArrayFeatureGate_Helpers(t *testing.T) {
	for _, tc := range gatedSubFields() {
		t.Run(tc.name, func(t *testing.T) {
			setArrayFeatureGate(t, true)
			err := checkArrayFeatureGateField(tc.field())
			requireGateRejects(t, err)
			assert.Contains(t, err.Error(), arrayFeatureGateMessage)
			requireGateRejects(t, checkArrayFeatureGateStruct(gateTestSchema(tc.field()).GetStructArrayFields()[0]))
			requireGateRejects(t, checkArrayFeatureGate(gateTestSchema(tc.field())))

			top := tc.field()
			requireGateRejects(t, checkArrayFeatureGate(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{top}}))

			setArrayFeatureGate(t, false)
			assert.NoError(t, checkArrayFeatureGateField(tc.field()))
			assert.NoError(t, checkArrayFeatureGate(gateTestSchema(tc.field())))
		})
	}

	t.Run("plain fields pass", func(t *testing.T) {
		setArrayFeatureGate(t, true)
		plain := &schemapb.FieldSchema{
			Name: "ints", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
		}
		assert.NoError(t, checkArrayFeatureGateField(plain))
		assert.NoError(t, checkArrayFeatureGate(gateTestSchema(plain)))
		assert.NoError(t, checkArrayFeatureGateField(nil))
	})
}

func TestArrayFeatureGate_CreateCollection(t *testing.T) {
	ctx := context.Background()
	newTask := func(t *testing.T, schema *schemapb.CollectionSchema) *CreateCollectionTask {
		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{}, nil).Maybe()
		marshaled, err := proto.Marshal(schema)
		require.NoError(t, err)
		task := &CreateCollectionTask{
			Condition: NewTaskCondition(ctx),
			CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
				CollectionName: schema.GetName(),
				Schema:         marshaled,
				ShardsNum:      common.DefaultShardsNum,
			},
			ctx:      ctx,
			mixCoord: NewMixCoordMock(),
		}
		task.MetaCache = mockCache
		require.NoError(t, task.OnEnqueue())
		return task
	}

	for _, tc := range gatedSubFields() {
		t.Run(tc.name, func(t *testing.T) {
			setArrayFeatureGate(t, true)
			requireGateRejects(t, newTask(t, gateTestSchema(tc.field())).PreExecute(ctx))

			setArrayFeatureGate(t, false)
			requireNotGated(t, newTask(t, gateTestSchema(tc.field())).PreExecute(ctx))
		})
	}

	t.Run("plain struct passes", func(t *testing.T) {
		setArrayFeatureGate(t, true)
		plain := &schemapb.FieldSchema{
			Name: "ints", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}},
		}
		assert.NoError(t, newTask(t, gateTestSchema(plain)).PreExecute(ctx))
	})
}

func TestArrayFeatureGate_AddCollectionStructField(t *testing.T) {
	ctx := context.Background()
	newTask := func(subField *schemapb.FieldSchema) *AddCollectionStructFieldTask {
		return &AddCollectionStructFieldTask{
			Condition: NewTaskCondition(ctx),
			AddCollectionStructFieldRequest: &milvuspb.AddCollectionStructFieldRequest{
				CollectionName: "gate_collection",
				StructArrayFieldSchema: &schemapb.StructArrayFieldSchema{
					Name:     "s",
					Nullable: true,
					Fields:   []*schemapb.FieldSchema{subField},
				},
			},
			ctx:       ctx,
			mixCoord:  NewMixCoordMock(),
			oldSchema: gateTestSchema(nil),
		}
	}

	for _, tc := range gatedSubFields() {
		t.Run(tc.name, func(t *testing.T) {
			setArrayFeatureGate(t, true)
			task := newTask(tc.field())
			require.NoError(t, task.OnEnqueue())
			requireGateRejects(t, task.PreExecute(ctx))

			setArrayFeatureGate(t, false)
			task = newTask(tc.field())
			require.NoError(t, task.OnEnqueue())
			requireNotGated(t, task.PreExecute(ctx))
		})
	}
}

// Top-level element-nullable and nested Array fields are rejected by the
// existing field validation; the gate is a second line behind it.
func topLevelGatedFields() []gatedSubField {
	return []gatedSubField{
		{"top-level nested array", func() *schemapb.FieldSchema {
			field := gatedSubFields()[0].field()
			field.Nullable = true
			return field
		}},
		{"top-level element-nullable array", func() *schemapb.FieldSchema {
			field := gatedSubFields()[1].field()
			field.Nullable = true
			return field
		}},
	}
}

func TestArrayFeatureGate_AddCollectionField(t *testing.T) {
	ctx := context.Background()
	for _, tc := range topLevelGatedFields() {
		t.Run(tc.name, func(t *testing.T) {
			setArrayFeatureGate(t, true)
			marshaled, err := proto.Marshal(tc.field())
			require.NoError(t, err)
			task := &AddCollectionFieldTask{
				Condition: NewTaskCondition(ctx),
				AddCollectionFieldRequest: &milvuspb.AddCollectionFieldRequest{
					CollectionName: "gate_collection",
					Schema:         marshaled,
				},
				ctx:       ctx,
				mixCoord:  NewMixCoordMock(),
				oldSchema: gateTestSchema(nil),
			}
			err = task.PreExecute(ctx)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestArrayFeatureGate_AlterCollectionSchemaAddField(t *testing.T) {
	ctx := context.Background()
	for _, tc := range topLevelGatedFields() {
		t.Run(tc.name, func(t *testing.T) {
			setArrayFeatureGate(t, true)
			task := &AlterCollectionSchemaTask{
				ctx:       ctx,
				oldSchema: gateTestSchema(nil),
				AlterCollectionSchemaRequest: &milvuspb.AlterCollectionSchemaRequest{
					CollectionName: "gate_collection",
					Action: &milvuspb.AlterCollectionSchemaRequest_Action{
						Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
							AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
								FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{FieldSchema: tc.field()}},
							},
						},
					},
				},
			}
			err := task.PreExecute(ctx)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func TestArrayFeatureGate_Import(t *testing.T) {
	ctx := GetContext(context.Background(), "root:123456")
	for _, tc := range gatedSubFields() {
		for _, file := range []struct {
			name    string
			path    string
			options []*commonpb.KeyValuePair
		}{
			{"json", "staging/file.json", nil},
			{"parquet", "staging/file.parquet", nil},
			{"csv", "staging/file.csv", nil},
			{"backup", "staging/backup", []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}}},
		} {
			t.Run(tc.name+"/"+file.name, func(t *testing.T) {
				newTask := func() *ImportTask {
					schema := gateTestSchema(tc.field())
					schema.StructArrayFields[0].Fields[0].Name = "s[" + schema.StructArrayFields[0].Fields[0].GetName() + "]"
					it := newImportTaskForPreExecute(t, file.options, schema)
					it.req.Files = []*internalpb.ImportFile{{Id: 1, Paths: []string{file.path}}}
					it.req.PartitionName = "_default"
					return it
				}
				setArrayFeatureGate(t, true)
				requireGateRejects(t, newTask().PreExecute(ctx))

				setArrayFeatureGate(t, false)
				requireNotGated(t, newTask().PreExecute(ctx))
			})
		}
	}
}

func TestArrayFeatureGate_AlterCollectionSchemaAddStructSubField(t *testing.T) {
	ctx := context.Background()
	for _, tc := range gatedSubFields() {
		t.Run(tc.name, func(t *testing.T) {
			newTask := func() *AlterCollectionSchemaTask {
				// An added sub-field must be element-nullable; a nested Array
				// mirrors that on its second level.
				field := tc.field()
				field.ElementNullable = true
				if element := field.GetTypeSchema().GetArrayElement(); element != nil {
					element.Nullable = true
				}
				return &AlterCollectionSchemaTask{
					ctx:       ctx,
					oldSchema: structSubFieldDDLTestSchema(true),
					AlterCollectionSchemaRequest: &milvuspb.AlterCollectionSchemaRequest{
						Action: &milvuspb.AlterCollectionSchemaRequest_Action{
							Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
								AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
									FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{
										StructPath:  "profile",
										FieldSchema: field,
									}},
								},
							},
						},
					},
				}
			}
			setArrayFeatureGate(t, true)
			err := newTask().PreExecute(ctx)
			requireGateRejects(t, err)
			assert.Contains(t, err.Error(), arrayFeatureGateMessage)

			setArrayFeatureGate(t, false)
			requireNotGated(t, newTask().PreExecute(ctx))
		})
	}
}
