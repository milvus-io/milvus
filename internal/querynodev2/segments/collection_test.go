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
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CollectionManagerSuite struct {
	suite.Suite
	cm *collectionManager
}

func (s *CollectionManagerSuite) SetupSuite() {
	paramtable.Init()
	initcore.InitLocalChunkManager("CollectionManagerSuite")
	initcore.InitMmapManager(paramtable.Get(), 1)
}

func (s *CollectionManagerSuite) SetupTest() {
	s.cm = NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("collection_1", schemapb.DataType_Int64, false)
	err := s.cm.PutOrRef(1, schema, mock_segcore.GenTestIndexMeta(1, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection,
	})
	s.Require().NoError(err)
}

func (s *CollectionManagerSuite) TearDownTest() {
	releaseAllTestCollections(s.cm)
}

func releaseAllTestCollections(manager CollectionManager) {
	for _, collectionID := range manager.List() {
		for !manager.Unref(collectionID, 1) {
		}
	}
}

func (s *CollectionManagerSuite) newSimpleRetrieveRequest(collection *Collection) *querypb.QueryRequest {
	pkField, err := typeutil.GetPrimaryFieldSchema(collection.Schema())
	s.Require().NoError(err)

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  pkField.GetFieldID(),
							DataType: pkField.GetDataType(),
						},
						Values: []*planpb.GenericValue{
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 1,
								},
							},
						},
					},
				},
			},
		},
		OutputFieldIds: []int64{pkField.GetFieldID()},
	}
	planBytes, err := proto.Marshal(planNode)
	s.Require().NoError(err)

	return &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Retrieve,
				MsgID:   100,
			},
			CollectionID:       collection.ID(),
			SerializedExprPlan: planBytes,
			MvccTimestamp:      1000,
		},
	}
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
		s.True(proto.Equal(schemaV8, updatedSchema))
	})

	s.Run("same_schema_version_updates_runtime_ttl_by_barrier", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("same_version", schemapb.DataType_Int64, false)
		baseSchema.Version = 9
		const (
			ttlFieldA = int64(91020)
			ttlFieldB = int64(91021)
		)
		baseSchema.Fields = append(baseSchema.Fields,
			&schemapb.FieldSchema{
				FieldID:  ttlFieldA,
				Name:     "ttl_a",
				DataType: schemapb.DataType_Timestamptz,
				Nullable: true,
			},
			&schemapb.FieldSchema{
				FieldID:  ttlFieldB,
				Name:     "ttl_b",
				DataType: schemapb.DataType_Timestamptz,
				Nullable: true,
			},
		)
		s.Require().NoError(cm.PutOrRef(91000, baseSchema, nil, &querypb.LoadMetaInfo{
			SchemaBarrierTs: 10,
			LogicalSchema:   baseSchema,
		}))
		defer cm.Unref(91000, 1)

		collection := cm.Get(91000)
		initial := collection.schema.Load()
		s.NotNil(initial.schemaRef)
		s.Equal(int64(-1), initial.EntityTTLFieldID())

		withTTL := func(fieldName string) *schemapb.CollectionSchema {
			updated := proto.Clone(baseSchema).(*schemapb.CollectionSchema)
			updated.Properties = append(updated.Properties, &commonpb.KeyValuePair{
				Key:   common.CollectionTTLFieldKey,
				Value: fieldName,
			})
			return updated
		}

		s.Require().NoError(cm.UpdateSchema(91000, withTTL("ttl_a"), 20))
		afterSet := collection.schema.Load()
		s.Same(initial.logicalSchema, afterSet.logicalSchema)
		s.Equal(ttlFieldA, afterSet.EntityTTLFieldID())
		s.Equal(uint64(20), afterSet.schemaBarrierTs)

		s.Require().NoError(cm.UpdateSchema(91000, withTTL("ttl_b"), 30))
		afterSwitch := collection.schema.Load()
		s.Same(initial.logicalSchema, afterSwitch.logicalSchema)
		s.Equal(ttlFieldB, afterSwitch.EntityTTLFieldID())
		s.Equal(uint64(30), afterSwitch.schemaBarrierTs)

		s.Require().NoError(cm.UpdateSchema(91000, baseSchema, 40))
		afterRemove := collection.schema.Load()
		s.Same(initial.logicalSchema, afterRemove.logicalSchema)
		s.Equal(int64(-1), afterRemove.EntityTTLFieldID())
		s.Equal(uint64(40), afterRemove.schemaBarrierTs)

		// An older replay cannot restore a TTL configuration already removed by
		// a newer barrier.
		stale := withTTL("ttl_a")
		s.Require().NoError(cm.UpdateSchema(91000, stale, 35))
		afterStale := collection.schema.Load()
		s.Same(afterRemove, afterStale)
		s.Equal(int64(-1), afterStale.EntityTTLFieldID())
	})

	s.Run("same_schema_version_updates_external_load_state", func() {
		cm := NewCollectionManager()
		baseSchema := mock_segcore.GenTestCollectionSchema("external_refresh", schemapb.DataType_Int64, false)
		baseSchema.Version = 10
		baseSchema.ExternalSource = "s3://old-bucket/table"
		baseSchema.ExternalSpec = `{"format":"parquet"}`
		s.Require().NoError(cm.PutOrRef(91005, baseSchema, nil, &querypb.LoadMetaInfo{
			SchemaBarrierTs: 10,
			LogicalSchema:   baseSchema,
		}))
		defer cm.Unref(91005, 1)

		updated := proto.Clone(baseSchema).(*schemapb.CollectionSchema)
		updated.ExternalSource = "s3://new-bucket/table"
		updated.ExternalSpec = `{"format":"milvus-table"}`
		s.Require().NoError(cm.UpdateSchema(91005, updated, 20))

		state, err := cm.Get(91005).CaptureSchemaState()
		s.Require().NoError(err)
		defer state.Release()
		// The logical cache entry remains keyed only by collection/version,
		// while the storage-facing snapshot follows the barrier-ordered update.
		s.Equal("s3://old-bucket/table", state.Schema().GetExternalSource())
		s.Equal("s3://new-bucket/table", state.LoadSchema().GetExternalSource())
		s.Equal(`{"format":"milvus-table"}`, state.LoadSchema().GetExternalSpec())
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
	coll.setSchema(&CollectionSchemaState{
		logicalSchema: schema,
		loadSchema:    schema,
	})

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
		schema.Version = int32(i)
		coll.setSchema(&CollectionSchemaState{
			logicalSchema:   schema,
			loadSchema:      schema,
			schemaBarrierTs: uint64(i),
		})
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

func (s *CollectionManagerSuite) TestPutOrRefUpdateIndexMetaWaitsForCollectionNativeLock() {
	coll := s.cm.Get(1)
	s.Require().NotNil(coll)

	schema := proto.Clone(coll.Schema()).(*schemapb.CollectionSchema)
	indexMeta := proto.Clone(coll.GetCCollection().IndexMeta()).(*segcorepb.CollectionIndexMeta)
	indexMeta.MaxIndexRowCount++

	coll.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- s.cm.PutOrRef(1, schema, indexMeta, &querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
	}()

	select {
	case err := <-done:
		coll.mu.Unlock()
		s.Require().NoError(err)
		s.FailNow("PutOrRef updated index meta while collection native lock was held")
	case <-time.After(50 * time.Millisecond):
	}

	coll.mu.Unlock()
	s.Require().NoError(<-done)
	s.cm.Unref(1, 1)
}

func (s *CollectionManagerSuite) TestCollectionNativeWrapperMethods() {
	coll := s.cm.Get(1)
	s.Require().NotNil(coll)

	searchReqPB, err := mock_segcore.GenQueryRequest(coll.GetCCollection(), nil, 1, 1, coll.ID())
	s.Require().NoError(err)
	searchReq, err := coll.NewSearchRequest(searchReqPB, searchReqPB.GetReq().GetPlaceholderGroup())
	s.Require().NoError(err)
	searchReq.Delete()

	retrievePlan, err := coll.NewRetrievePlan(s.newSimpleRetrieveRequest(coll))
	s.Require().NoError(err)
	retrievePlan.Delete()

	csegment, err := coll.CreateCSegment(&segcore.CreateCSegmentRequest{
		SegmentID:   1,
		SegmentType: SegmentTypeSealed,
	})
	s.Require().NoError(err)
	releaser, ok := csegment.(interface{ Release() })
	s.Require().True(ok)
	releaser.Release()

	s.NoError(coll.updateIndexMeta(nil))
}

func (s *CollectionManagerSuite) TestCollectionNativeWrapperMethodsReleased() {
	coll := s.cm.Get(1)
	s.Require().NotNil(coll)

	searchReqPB, err := mock_segcore.GenQueryRequest(coll.GetCCollection(), nil, 1, 1, coll.ID())
	s.Require().NoError(err)
	retrieveReq := s.newSimpleRetrieveRequest(coll)
	indexMeta := mock_segcore.GenTestIndexMeta(1, coll.Schema())

	DeleteCollection(coll)

	_, err = coll.NewSearchRequest(searchReqPB, searchReqPB.GetReq().GetPlaceholderGroup())
	s.Error(err)
	_, err = coll.NewRetrievePlan(retrieveReq)
	s.Error(err)
	_, err = coll.CreateCSegment(&segcore.CreateCSegmentRequest{
		SegmentID:   1,
		SegmentType: SegmentTypeSealed,
	})
	s.Error(err)
	s.Error(coll.updateIndexMeta(indexMeta))
	s.Error(coll.updateSchema(coll.Schema(), nil))
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
	s.True(proto.Equal(updatedSchema, schema))
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
		defer releaseAllTestCollections(cm)
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

func userLoadFields(schema *schemapb.CollectionSchema) []int64 {
	fields := make([]int64, 0)
	for _, field := range schema.GetFields() {
		if !common.IsSystemField(field.GetFieldID()) {
			fields = append(fields, field.GetFieldID())
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			if !common.IsSystemField(field.GetFieldID()) {
				fields = append(fields, field.GetFieldID())
			}
		}
	}
	return fields
}

func (s *CollectionManagerSuite) TestSameVersionSameBarrierRefreshesLoadFields() {
	cm := NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("load_fields_refresh", schemapb.DataType_Int64, false)
	schema.Version = 7
	fields := userLoadFields(schema)
	s.Require().GreaterOrEqual(len(fields), 2)
	loadMeta := func(fieldID int64) *querypb.LoadMetaInfo {
		return &querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			LoadFields:      []int64{fieldID},
			SchemaBarrierTs: 100,
			LogicalSchema:   schema,
		}
	}

	s.Require().NoError(cm.PutOrRef(91001, schema, nil, loadMeta(fields[0])))
	s.Require().NoError(cm.PutOrRef(91001, schema, nil, loadMeta(fields[1])))
	defer cm.Unref(91001, 2)

	state, err := cm.Get(91001).CaptureSchemaState()
	s.Require().NoError(err)
	defer state.Release()
	s.ElementsMatch([]int64{fields[1]}, state.LoadFields())
}

func (s *CollectionManagerSuite) TestPutOrRefWithSchemaStateSharesRepeatedLoadSnapshot() {
	const collectionID = int64(91006)
	schema := mock_segcore.GenTestCollectionSchema("shared_load_state", schemapb.DataType_Int64, false)
	schema.Version = 7
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
		LogicalSchema:   schema,
	}

	cm := NewCollectionManager()
	first, err := cm.PutOrRefWithSchemaState(collectionID, schema, nil, loadMeta)
	s.Require().NoError(err)
	defer first.Release()
	second, err := cm.PutOrRefWithSchemaState(collectionID, schema, nil, loadMeta)
	s.Require().NoError(err)
	defer second.Release()
	defer cm.Unref(collectionID, 2)

	// CollectionSchemaState.Clone keeps the immutable protos and native shared
	// pointers; repeated per-segment RPCs must not parse a new load schema.
	s.Same(first.Schema(), second.Schema())
	s.Same(first.LoadSchema(), second.LoadSchema())

	published, err := cm.Get(collectionID).CaptureSchemaState()
	s.Require().NoError(err)
	defer published.Release()
	s.Same(first.LoadSchema(), published.LoadSchema())
}

func (s *CollectionManagerSuite) TestPutOrRefWithSchemaStateKeepsStaleVChannelSnapshotPrivate() {
	const collectionID = int64(91007)
	currentSchema := mock_segcore.GenTestCollectionSchema("current_load_state", schemapb.DataType_Int64, false)
	currentSchema.Version = 2
	staleSchema := mock_segcore.GenTestCollectionSchema("stale_load_state", schemapb.DataType_Int64, false)
	staleSchema.Version = 1

	cm := NewCollectionManager()
	current, err := cm.PutOrRefWithSchemaState(collectionID, currentSchema, nil, &querypb.LoadMetaInfo{
		SchemaBarrierTs: 200,
		LogicalSchema:   currentSchema,
	})
	s.Require().NoError(err)
	defer current.Release()
	stale, err := cm.PutOrRefWithSchemaState(collectionID, staleSchema, nil, &querypb.LoadMetaInfo{
		SchemaBarrierTs: 100,
		LogicalSchema:   staleSchema,
	})
	s.Require().NoError(err)
	defer stale.Release()
	defer cm.Unref(collectionID, 2)

	s.Equal(uint64(1), stale.Version())
	s.Equal(uint64(100), stale.BarrierTs())
	published, err := cm.Get(collectionID).CaptureSchemaState()
	s.Require().NoError(err)
	defer published.Release()
	s.Equal(uint64(2), published.Version())
	s.Equal(uint64(200), published.BarrierTs())
}

func (s *CollectionManagerSuite) TestSchemaAddFieldPreservesFullVsPartialLoadIntent() {
	base := mock_segcore.GenTestCollectionSchema("load_intent", schemapb.DataType_Int64, false)
	base.Version = 11
	fields := userLoadFields(base)
	s.Require().GreaterOrEqual(len(fields), 2)
	updated := proto.Clone(base).(*schemapb.CollectionSchema)
	updated.Version++
	const addedFieldID = int64(91010)
	updated.Fields = append(updated.Fields, &schemapb.FieldSchema{
		FieldID:  addedFieldID,
		Name:     "new_field",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})

	for _, test := range []struct {
		name            string
		collectionID    int64
		loadFields      []int64
		wantAddedLoaded bool
	}{
		{name: "full", collectionID: 91002, loadFields: fields, wantAddedLoaded: true},
		{name: "partial", collectionID: 91003, loadFields: fields[:1], wantAddedLoaded: false},
	} {
		s.Run(test.name, func() {
			cm := NewCollectionManager()
			s.Require().NoError(cm.PutOrRef(test.collectionID, base, nil, &querypb.LoadMetaInfo{
				LoadType:        querypb.LoadType_LoadCollection,
				LoadFields:      test.loadFields,
				SchemaBarrierTs: 10,
				LogicalSchema:   base,
			}))
			defer cm.Unref(test.collectionID, 1)
			s.Require().NoError(cm.UpdateSchema(test.collectionID, updated, 20))

			state, err := cm.Get(test.collectionID).CaptureSchemaState()
			s.Require().NoError(err)
			defer state.Release()
			_, err = getFieldSchema(state.LoadSchema(), addedFieldID)
			s.Require().NoError(err)
			if test.wantAddedLoaded {
				s.Empty(state.LoadFields(), "an empty load-field set is the full-load representation")
			} else {
				s.ElementsMatch(test.loadFields, state.LoadFields())
				s.NotContains(state.LoadFields(), addedFieldID)
			}
		})
	}
}

func (s *CollectionManagerSuite) TestSchemaAddFieldPreservesEffectiveLoadPolicy() {
	const (
		collectionID = int64(91004)
		addedFieldID = int64(91011)
	)
	logical := mock_segcore.GenTestCollectionSchema("load_policy", schemapb.DataType_Int64, false)
	logical.Version = 20
	effective := proto.Clone(logical).(*schemapb.CollectionSchema)
	effective.Properties = append(effective.Properties,
		&commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "true"},
		&commonpb.KeyValuePair{Key: common.WarmupScalarFieldKey, Value: common.WarmupSync},
	)
	materializeLoadPolicy(effective)

	cm := NewCollectionManager()
	s.Require().NoError(cm.PutOrRef(collectionID, effective, nil, &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
		LogicalSchema:   logical,
	}))
	defer cm.Unref(collectionID, 1)

	updated := proto.Clone(logical).(*schemapb.CollectionSchema)
	updated.Version++
	updated.Fields = append(updated.Fields, &schemapb.FieldSchema{
		FieldID:  addedFieldID,
		Name:     "new_scalar",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})
	s.Require().NoError(cm.UpdateSchema(collectionID, updated, 110))

	state, err := cm.Get(collectionID).CaptureSchemaState()
	s.Require().NoError(err)
	defer state.Release()
	field, err := getFieldSchema(state.LoadSchema(), addedFieldID)
	s.Require().NoError(err)
	mmapEnabled, hasMmap := common.IsMmapDataEnabled(field.GetTypeParams()...)
	s.True(hasMmap)
	s.True(mmapEnabled)
	warmup, hasWarmup := common.GetWarmupPolicy(field.GetTypeParams()...)
	s.True(hasWarmup)
	s.Equal(common.WarmupSync, warmup)
}

func (s *CollectionManagerSuite) TestLogicalLoadPolicyUpdateOverridesPreviousEffectivePolicy() {
	logical := mock_segcore.GenTestCollectionSchema("load_policy_update", schemapb.DataType_Int64, false)
	effective := proto.Clone(logical).(*schemapb.CollectionSchema)
	effective.Properties = append(effective.Properties,
		&commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "true"},
	)

	updated := proto.Clone(logical).(*schemapb.CollectionSchema)
	updated.Properties = append(updated.Properties,
		&commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "false"},
	)
	result := evolveLoadSchema(updated, &CollectionSchemaState{
		logicalSchema: logical,
		loadSchema:    effective,
	})

	mmapEnabled, hasMmap := common.IsMmapDataEnabled(result.GetProperties()...)
	s.True(hasMmap)
	s.False(mmapEnabled)
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
