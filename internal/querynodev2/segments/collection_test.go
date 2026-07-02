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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CollectionManagerSuite struct {
	suite.Suite
	cm *collectionManager
}

func (s *CollectionManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CollectionManagerSuite) SetupTest() {
	s.cm = NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	err := s.cm.PutOrRef(1, schema, mock_segcore.GenTestIndexMeta(1, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	s.Require().NoError(err)
}

func (s *CollectionManagerSuite) TestUpdateSchema() {
	s.Run("normal_case", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		schema.Version = 100
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(len(schema.Fields)),
			Name:     "added_field",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		err := s.cm.UpdateSchema(1, schema, 100)
		s.NoError(err)
		s.Equal(uint64(100), s.cm.Get(1).SchemaVersion())
	})

	s.Run("stale_version", func() {
		currentSchema, currentVersion := s.cm.Get(1).SchemaAndVersion()
		staleSchema := mock_segcore.GenTestCollectionSchema("stale_collection", schemapb.DataType_Int64, false)
		staleSchema.Version = int32(currentVersion - 1)

		err := s.cm.UpdateSchema(1, staleSchema, currentVersion+1)
		s.NoError(err)

		updatedSchema, updatedVersion := s.cm.Get(1).SchemaAndVersion()
		s.Equal(currentVersion, updatedVersion)
		s.Same(currentSchema, updatedSchema)
	})

	s.Run("stale_schema_version_with_larger_timestamp", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("collection_v7", schemapb.DataType_Int64, false)
		baseSchema.Version = 7
		err := cm.PutOrRef(10, baseSchema, mock_segcore.GenTestIndexMeta(10, baseSchema), &querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			SchemaBarrierTs: 50,
		})
		s.Require().NoError(err)
		defer cm.Unref(10, 1)

		schemaV8 := mock_segcore.GenTestCollectionSchema("collection_v8", schemapb.DataType_Int64, false)
		schemaV8.Version = 8
		schemaV8.Fields = append(schemaV8.Fields, &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(len(schemaV8.Fields)),
			Name:     "field_v8",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		err = cm.UpdateSchema(10, schemaV8, 8)
		s.NoError(err)
		s.Equal(uint64(8), cm.Get(10).SchemaVersion())

		schemaV7 := mock_segcore.GenTestCollectionSchema("collection_v7", schemapb.DataType_Int64, false)
		schemaV7.Version = 7

		err = cm.UpdateSchema(10, schemaV7, 200)
		s.NoError(err)

		updatedSchema, updatedVersion := cm.Get(10).SchemaAndVersion()
		s.Equal(uint64(8), updatedVersion)
		s.Same(schemaV8, updatedSchema)
	})

	s.Run("same_schema_version_with_newer_barrier_updates_properties", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(10, baseSchema, mock_segcore.GenTestIndexMeta(10, baseSchema), &querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			SchemaBarrierTs: 50,
		})
		s.Require().NoError(err)
		defer cm.Unref(10, 1)

		updatedSchema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
		updatedSchema.Version = baseSchema.GetVersion()
		updatedSchema.Properties = []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLFieldKey, Value: "int64Field"},
		}

		err = cm.UpdateSchema(10, updatedSchema, 100)
		s.NoError(err)

		schema, version := cm.Get(10).SchemaAndVersion()
		s.Equal(uint64(0), version)
		s.Same(updatedSchema, schema)
		s.Equal("int64Field", common.CloneKeyValuePairs(schema.GetProperties()).ToMap()[common.CollectionTTLFieldKey])
	})

	s.Run("higher_schema_version_after_high_barrier_refresh_uses_monotonic_segcore_schema_version", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(10, baseSchema, mock_segcore.GenTestIndexMeta(10, baseSchema), &querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			SchemaBarrierTs: 100,
		})
		s.Require().NoError(err)
		defer cm.Unref(10, 1)

		schemaV1 := mock_segcore.GenTestCollectionSchema("collection_v1", schemapb.DataType_Int64, false)
		schemaV1.Version = 1
		plan, shouldUpdate := prepareCollectionSchemaUpdate(cm.Get(10), uint64(schemaV1.GetVersion()), 80)
		s.True(shouldUpdate)
		s.Equal(uint64(1), plan.logicalSchemaVersion)
		s.Equal(uint64(100), plan.schemaBarrierTs)
		s.Equal(uint64(101), plan.segcoreSchemaVersion)

		cm.Get(10).setSchema(schemaV1, plan.logicalSchemaVersion, plan.schemaBarrierTs, plan.segcoreSchemaVersion)
		schemaV2 := mock_segcore.GenTestCollectionSchema("collection_v2", schemapb.DataType_Int64, false)
		schemaV2.Version = 2
		plan, shouldUpdate = prepareCollectionSchemaUpdate(cm.Get(10), uint64(schemaV2.GetVersion()), 80)
		s.True(shouldUpdate)
		s.Equal(uint64(2), plan.logicalSchemaVersion)
		s.Equal(uint64(100), plan.schemaBarrierTs)
		s.Equal(uint64(102), plan.segcoreSchemaVersion)
	})

	s.Run("manager_uses_schema_version_from_caller", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(10, baseSchema, mock_segcore.GenTestIndexMeta(10, baseSchema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.Require().NoError(err)
		defer cm.Unref(10, 1)

		schema := mock_segcore.GenTestCollectionSchema("collection_v2", schemapb.DataType_Int64, false)
		schema.Version = 2
		err = cm.UpdateSchema(10, schema, 2)
		s.NoError(err)

		_, version := cm.Get(10).SchemaAndVersion()
		s.Equal(uint64(2), version)
	})

	s.Run("not_exist_collection", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + int64(len(schema.Fields)),
			Name:     "added_field",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		err := s.cm.UpdateSchema(2, schema, 100)
		s.Error(err)
	})

	s.Run("nil_schema", func() {
		s.NotPanics(func() {
			err := s.cm.UpdateSchema(1, nil, 101)
			s.Error(err)
		})
	})
}

func (s *CollectionManagerSuite) TestSchemaAndVersionSnapshot() {
	coll := s.cm.Get(1)
	schema := mock_segcore.GenTestCollectionSchema("collection_0", schemapb.DataType_Int64, false)
	coll.setSchema(schema, 0, 0, initialSegcoreSchemaVersion(0, 0))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan string, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			schema, version := coll.SchemaAndVersion()
			if schema.GetName() != fmt.Sprintf("collection_%d", version) {
				select {
				case errCh <- fmt.Sprintf("schema %s does not match version %d", schema.GetName(), version):
				default:
				}
				return
			}
		}
	}()

	for i := 1; i <= 1000; i++ {
		schema := mock_segcore.GenTestCollectionSchema(fmt.Sprintf("collection_%d", i), schemapb.DataType_Int64, false)
		coll.setSchema(schema, uint64(i), uint64(i), initialSegcoreSchemaVersion(uint64(i), uint64(i)))
	}
	close(stop)
	wg.Wait()

	select {
	case msg := <-errCh:
		s.Fail(msg)
	default:
	}

	schema, version := coll.SchemaAndVersion()
	s.Equal(uint64(1000), version)
	s.Equal("collection_1000", schema.GetName())
}

func (s *CollectionManagerSuite) TestNewMilvusTableCollectionKeepsUserSchema() {
	schema := milvusTableCollectionSchema(false)
	collection, err := NewCollection(10, schema, nil, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	s.Require().NoError(err)
	defer DeleteCollection(collection)

	s.Equal("id", collection.Schema().GetFields()[0].GetExternalField())
	s.Equal("embedding", collection.Schema().GetFields()[1].GetExternalField())
	s.Equal("id", collection.GetCCollection().Schema().GetFields()[0].GetExternalField())
	s.Equal("embedding", collection.GetCCollection().Schema().GetFields()[1].GetExternalField())
}

func (s *CollectionManagerSuite) TestPutOrRefUpdateIndexMeta() {
	// Verify initial collection has IndexMeta set from SetupTest.
	coll := s.cm.Get(1)
	s.Require().NotNil(coll)
	s.Require().NotNil(coll.GetCCollection().IndexMeta())

	// Add a new vector field to simulate schema evolution.
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	schema.Version = 2
	newVecFieldID := int64(200)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:  newVecFieldID,
		Name:     "new_float_vector",
		DataType: schemapb.DataType_FloatVector,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "128"},
		},
	})

	// Build IndexMeta from the updated schema (should include the new field).
	newIndexMeta := mock_segcore.GenTestIndexMeta(1, schema)
	hasNewField := false
	for _, meta := range newIndexMeta.GetIndexMetas() {
		if meta.GetFieldID() == newVecFieldID {
			hasNewField = true
			break
		}
	}
	s.Require().True(hasNewField, "precondition: new IndexMeta should contain field %d", newVecFieldID)

	// PutOrRef on an existing collection should update its IndexMeta.
	err := s.cm.PutOrRef(1, schema, newIndexMeta, &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	})
	s.Require().NoError(err)
	defer s.cm.Unref(1, 1)

	updatedCollection := s.cm.Get(1)
	updatedSchema, updatedVersion := updatedCollection.SchemaAndVersion()
	s.Equal(uint64(2), updatedVersion)
	s.Len(updatedSchema.GetFields(), len(schema.GetFields()))

	// Verify IndexMeta now contains the new field.
	updatedIndexMeta := updatedCollection.GetCCollection().IndexMeta()
	found := false
	for _, meta := range updatedIndexMeta.GetIndexMetas() {
		if meta.GetFieldID() == newVecFieldID {
			found = true
			break
		}
	}
	s.True(found,
		"PutOrRef should update IndexMeta for existing collections; field %d is missing",
		newVecFieldID)
}

func (s *CollectionManagerSuite) TestPutOrRefKeepsFreshCollectionInSchemaVersionDomain() {
	cm := NewCollectionManager()
	initialSchema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
	err := cm.PutOrRef(10, initialSchema, mock_segcore.GenTestIndexMeta(10, initialSchema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	})
	s.Require().NoError(err)
	defer cm.Unref(10, 1)

	_, version := cm.Get(10).SchemaAndVersion()
	s.Equal(uint64(0), version)

	updatedSchema := mock_segcore.GenTestCollectionSchema("collection_v1", schemapb.DataType_Int64, false)
	updatedSchema.Version = 1
	err = cm.UpdateSchema(10, updatedSchema, 200)
	s.Require().NoError(err)

	schema, version := cm.Get(10).SchemaAndVersion()
	s.Equal(uint64(1), version)
	s.Same(updatedSchema, schema)
}

func (s *CollectionManagerSuite) TestLoadMetaSchemaVersionCompatibility() {
	s.Run("use_schema_version_when_schema_is_present", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_v7", schemapb.DataType_Int64, false)
		schema.Version = 7
		loadMeta := &querypb.LoadMetaInfo{
			SchemaBarrierTs: 100,
		}

		s.Equal(uint64(7), getLoadMetaSchemaVersion(schema, loadMeta))
	})

	s.Run("keep_zero_schema_version_for_new_collection", func() {
		schema := mock_segcore.GenTestCollectionSchema("collection_v0", schemapb.DataType_Int64, false)
		loadMeta := &querypb.LoadMetaInfo{
			SchemaBarrierTs: 100,
		}

		s.Equal(uint64(0), getLoadMetaSchemaVersion(schema, loadMeta))
	})

	s.Run("fallback_to_legacy_barrier_without_schema", func() {
		loadMeta := &querypb.LoadMetaInfo{
			SchemaBarrierTs: 100,
		}

		s.Equal(uint64(100), getLoadMetaSchemaVersion(nil, loadMeta))
	})
}

func (s *CollectionManagerSuite) TestGpuIndexFlagWithCagraAdaptForCPU() {
	schema := mock_segcore.GenTestCollectionSchema("collection_cagra", schemapb.DataType_Int64, false)
	vectorFieldID := int64(0)
	for _, field := range schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_FloatVector {
			vectorFieldID = field.GetFieldID()
			break
		}
	}
	s.Require().NotZero(vectorFieldID)

	tests := []struct {
		name       string
		indexType  string
		adaptValue string
		expected   bool
	}{
		{
			name:       "GPU_CAGRA adapt for CPU",
			indexType:  "GPU_CAGRA",
			adaptValue: "true",
			expected:   false,
		},
		{
			name:       "GPU_CUVS_CAGRA adapt for CPU",
			indexType:  "GPU_CUVS_CAGRA",
			adaptValue: "1",
			expected:   false,
		},
		{
			name:      "GPU_CAGRA without adapt for CPU",
			indexType: "GPU_CAGRA",
			expected:  true,
		},
		{
			name:       "other GPU index",
			indexType:  "GPU_IVF_FLAT",
			adaptValue: "true",
			expected:   true,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			indexParams := []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: test.indexType},
				{Key: common.MetricTypeKey, Value: "L2"},
			}
			if test.adaptValue != "" {
				indexParams = append(indexParams, &commonpb.KeyValuePair{Key: "adapt_for_cpu", Value: test.adaptValue})
			}
			indexMeta := &segcorepb.CollectionIndexMeta{
				MaxIndexRowCount: 1,
				IndexMetas: []*segcorepb.FieldIndexMeta{
					{
						FieldID:     vectorFieldID,
						IndexName:   test.indexType,
						IndexParams: indexParams,
					},
				},
			}

			collection, err := NewCollection(10, schema, indexMeta, &querypb.LoadMetaInfo{
				LoadType: querypb.LoadType_LoadCollection,
			})
			s.Require().NoError(err)
			defer DeleteCollection(collection)
			s.Equal(test.expected, collection.IsGpuIndex())
		})
	}

	s.Run("GPU_CAGRA adapt for CPU from load config", func() {
		params := paramtable.Get()
		oldEnable := params.KnowhereConfig.Enable.GetValue()
		adaptKey := params.KnowhereConfig.IndexParam.KeyPrefix + "GPU_CAGRA.load.adapt_for_cpu"
		oldAdaptValue := params.GetWithDefault(adaptKey, "")
		defer params.Save(params.KnowhereConfig.Enable.Key, oldEnable)
		defer func() {
			if oldAdaptValue == "" {
				params.Remove(adaptKey)
				return
			}
			params.Save(adaptKey, oldAdaptValue)
		}()

		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(adaptKey, "true")

		indexMeta := &segcorepb.CollectionIndexMeta{
			MaxIndexRowCount: 1,
			IndexMetas: []*segcorepb.FieldIndexMeta{
				{
					FieldID:   vectorFieldID,
					IndexName: "GPU_CAGRA",
					IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
						{Key: common.MetricTypeKey, Value: "L2"},
					},
				},
			},
		}

		collection, err := NewCollection(10, schema, indexMeta, &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.Require().NoError(err)
		defer DeleteCollection(collection)
		s.False(collection.IsGpuIndex())
	})
}

func milvusTableCollectionSchema(withVirtualPK bool) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{
			FieldID:       100,
			Name:          "target_id",
			DataType:      schemapb.DataType_Int64,
			IsPrimaryKey:  !withVirtualPK,
			ExternalField: "id",
		},
		{
			FieldID:       101,
			Name:          "target_vector",
			DataType:      schemapb.DataType_FloatVector,
			ExternalField: "embedding",
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "4"},
			},
		},
	}
	if withVirtualPK {
		fields = append(fields, &schemapb.FieldSchema{
			FieldID:      102,
			Name:         common.VirtualPKFieldName,
			DataType:     schemapb.DataType_Int64,
			IsPrimaryKey: true,
			AutoID:       true,
		})
	}
	return &schemapb.CollectionSchema{
		Name:         "milvus_table_collection",
		ExternalSpec: `{"format":"milvus-table"}`,
		Fields:       fields,
	}
}

func (s *CollectionManagerSuite) TestRef() {
	s.Run("ref_existing_collection", func() {
		ok := s.cm.Ref(1, 1)
		s.True(ok)
	})

	s.Run("ref_non_existing_collection", func() {
		ok := s.cm.Ref(9999, 1)
		s.False(ok)
	})
}

func (s *CollectionManagerSuite) TestUnref() {
	s.Run("unref_non_existing_collection", func() {
		// Unref on non-existing collection should return true
		ok := s.cm.Unref(9999, 1)
		s.True(ok)
	})

	s.Run("unref_without_release", func() {
		// Add more refs first
		s.cm.Ref(1, 2)
		// Unref once, should not release (refCount > 0)
		ok := s.cm.Unref(1, 1)
		s.False(ok)
		// Collection should still exist
		coll := s.cm.Get(1)
		s.NotNil(coll)
	})

	s.Run("unref_with_release", func() {
		// Create a new collection manager for this test
		cm := NewCollectionManager()
		schema := mock_segcore.GenTestCollectionSchema("collection_2", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(2, schema, mock_segcore.GenTestIndexMeta(2, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.Require().NoError(err)

		// Unref to release the collection (refCount goes to 0)
		ok := cm.Unref(2, 1)
		s.True(ok)

		// Collection should be removed
		coll := cm.Get(2)
		s.Nil(coll)
	})
}

func (s *CollectionManagerSuite) TestList() {
	ids := s.cm.List()
	s.Contains(ids, int64(1))
}

func (s *CollectionManagerSuite) TestListWithName() {
	names := s.cm.ListWithName()
	s.Contains(names, int64(1))
	s.Equal("collection_1", names[1])
}

func (s *CollectionManagerSuite) TestPutOrRef() {
	s.Run("put_new_collection", func() {
		cm := NewCollectionManager()
		schema := mock_segcore.GenTestCollectionSchema("collection_new", schemapb.DataType_Int64, false)
		err := cm.PutOrRef(100, schema, mock_segcore.GenTestIndexMeta(100, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.NoError(err)
		coll := cm.Get(100)
		s.NotNil(coll)
	})

	s.Run("ref_existing_collection", func() {
		// Ref existing collection (id=1)
		schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
		err := s.cm.PutOrRef(1, schema, mock_segcore.GenTestIndexMeta(1, schema), &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
		s.NoError(err)
	})
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
