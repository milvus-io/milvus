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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
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

func holdInsertSchemaTransition(t *testing.T, collection *Collection) func() {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var releaseOnce sync.Once

	go func() {
		defer close(done)
		collection.WithInsertSchemaTransition(func(*schemapb.CollectionSchema) {
			close(entered)
			<-release
		})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("insert schema transition reader did not start")
	}

	return func() {
		releaseOnce.Do(func() {
			close(release)
		})
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("insert schema transition reader did not stop")
		}
	}
}

func waitForSchemaTransitionWriter(t *testing.T, collection *Collection) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	for {
		if !collection.schemaTransitionMu.TryRLock() {
			return
		}
		collection.schemaTransitionMu.RUnlock()

		select {
		case <-deadline.C:
			t.Fatal("schema writer did not queue behind the insert transition reader")
		default:
			runtime.Gosched()
		}
	}
}

func mockNativeSchemaUpdate(t *testing.T) <-chan struct{} {
	t.Helper()
	entered := make(chan struct{})
	var once sync.Once
	var origin func(*segcore.CCollection, *schemapb.CollectionSchema, uint64) error
	mock := mockey.Mock((*segcore.CCollection).UpdateSchema).To(func(c *segcore.CCollection, schema *schemapb.CollectionSchema, version uint64) error {
		once.Do(func() {
			close(entered)
		})
		return origin(c, schema, version)
	}).Origin(&origin).Build()
	t.Cleanup(func() {
		mock.UnPatch()
	})
	return entered
}

func (s *CollectionManagerSuite) assertNativeSchemaUpdateWaitsForTransitionReader(collection *Collection, update func() error) {
	releaseReader := holdInsertSchemaTransition(s.T(), collection)
	defer releaseReader()

	nativeEntered := mockNativeSchemaUpdate(s.T())
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- update()
	}()

	waitForSchemaTransitionWriter(s.T(), collection)

	select {
	case <-nativeEntered:
		s.T().Fatal("native schema update entered while an insert transition reader was held")
	default:
	}

	releaseReader()
	select {
	case <-nativeEntered:
	case <-time.After(5 * time.Second):
		s.T().Fatal("native schema update did not continue after the transition reader was released")
	}
	s.Require().NoError(<-updateDone)
}

func (s *CollectionManagerSuite) TestSchemaUpdateWaitsForTransitionReader() {
	s.Run("UpdateSchema", func() {
		coll := s.cm.Get(1)
		s.Require().NotNil(coll)

		schema := proto.Clone(coll.Schema()).(*schemapb.CollectionSchema)
		schema.Version++
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  580,
			Name:     "schema_transition_update",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		s.assertNativeSchemaUpdateWaitsForTransitionReader(coll, func() error {
			return s.cm.UpdateSchema(coll.ID(), schema, 1)
		})
	})

	s.Run("PutOrRef", func() {
		coll := s.cm.Get(1)
		s.Require().NotNil(coll)

		schema := proto.Clone(coll.Schema()).(*schemapb.CollectionSchema)
		schema.Version++
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:  581,
			Name:     "schema_transition_put_or_ref",
			DataType: schemapb.DataType_Bool,
			Nullable: true,
		})

		s.assertNativeSchemaUpdateWaitsForTransitionReader(coll, func() error {
			return s.cm.PutOrRef(coll.ID(), schema, nil, &querypb.LoadMetaInfo{
				CollectionID:    coll.ID(),
				LoadType:        querypb.LoadType_LoadCollection,
				SchemaBarrierTs: 2,
			})
		})
		s.cm.Unref(coll.ID(), 1)
	})
}

func (s *CollectionManagerSuite) TestSchemaUpdateDoesNotBlockUnrelatedCollectionGet() {
	otherSchema := mock_segcore.GenTestCollectionSchema("other_collection", schemapb.DataType_Int64, false)
	s.Require().NoError(s.cm.PutOrRef(2, otherSchema, nil, &querypb.LoadMetaInfo{
		CollectionID: 2,
		LoadType:     querypb.LoadType_LoadCollection,
	}))
	defer s.cm.Unref(2, 1)

	for _, test := range []struct {
		name   string
		update func(collection *Collection, schema *schemapb.CollectionSchema) error
		unref  bool
	}{
		{
			name: "UpdateSchema",
			update: func(collection *Collection, schema *schemapb.CollectionSchema) error {
				return s.cm.UpdateSchema(collection.ID(), schema, 1)
			},
		},
		{
			name: "PutOrRef",
			update: func(collection *Collection, schema *schemapb.CollectionSchema) error {
				return s.cm.PutOrRef(collection.ID(), schema, nil, &querypb.LoadMetaInfo{
					CollectionID:    collection.ID(),
					LoadType:        querypb.LoadType_LoadCollection,
					SchemaBarrierTs: 1,
				})
			},
			unref: true,
		},
	} {
		s.Run(test.name, func() {
			coll := s.cm.Get(1)
			schema := proto.Clone(coll.Schema()).(*schemapb.CollectionSchema)
			schema.Version++

			releaseReader := holdInsertSchemaTransition(s.T(), coll)
			defer releaseReader()
			updateDone := make(chan error, 1)
			go func() {
				updateDone <- test.update(coll, schema)
			}()

			waitForSchemaTransitionWriter(s.T(), coll)

			getDone := make(chan *Collection, 1)
			go func() {
				getDone <- s.cm.Get(2)
			}()
			select {
			case other := <-getDone:
				s.Require().NotNil(other)
			case <-time.After(5 * time.Second):
				s.T().Fatal("schema update on one collection blocked Get on another collection")
			}

			releaseReader()
			s.Require().NoError(<-updateDone)
			if test.unref {
				s.cm.Unref(coll.ID(), 1)
			}
		})
	}
}

func (s *CollectionManagerSuite) TestSchemaUpdateLeaseKeepsCollectionAliveWhileWaiting() {
	coll := s.cm.Get(1)
	schema := proto.Clone(coll.Schema()).(*schemapb.CollectionSchema)
	schema.Version++

	releaseReader := holdInsertSchemaTransition(s.T(), coll)
	defer releaseReader()
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- s.cm.UpdateSchema(coll.ID(), schema, 1)
	}()

	waitForSchemaTransitionWriter(s.T(), coll)

	unrefDone := make(chan bool, 1)
	go func() {
		unrefDone <- s.cm.Unref(coll.ID(), 1)
	}()
	select {
	case released := <-unrefDone:
		s.False(released, "the update lease must retain the collection")
	case <-time.After(5 * time.Second):
		s.T().Fatal("Unref blocked while schema update waited for transition reader")
	}
	s.Same(coll, s.cm.Get(coll.ID()))

	releaseReader()
	s.Require().NoError(<-updateDone)
	s.Nil(s.cm.Get(coll.ID()), "releasing the lease should complete the pending collection release")
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
	s.Error(coll.updateSchema(coll.Schema(), 1))
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

// TestLoadWinsRaceDoesNotSuppressStreamSchemaUpdate reproduces the add_function_field
// load-wins race (#50989 / #51062). The segment reopen path and the stream DDL path both
// advance the delegator collection schema, but only the stream path rebuilds the delegator's
// derived state (idfOracle / function runners). If a reopen advances the SERVED schema version
// first, the stream's later UpdateSchema at the same version is judged a no-op and the derived-
// state rebuild is skipped, leaving a newly added BM25/MinHash function field unsearchable.
//
// The applied-vs-served split makes the reopen path advance the per-segment applied schema +
// index meta but NOT the collection SERVED schema version, so a same-version stream UpdateSchema
// still applies and rebuilds derived state.
func (s *CollectionManagerSuite) TestLoadWinsRaceDoesNotSuppressStreamSchemaUpdate() {
	cm := NewCollectionManager()
	const collID = 20

	baseSchema := mock_segcore.GenTestCollectionSchema("load_wins_v1", schemapb.DataType_Int64, false)
	baseSchema.Version = 1
	s.Require().NoError(cm.PutOrRef(collID, baseSchema, mock_segcore.GenTestIndexMeta(collID, baseSchema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	}))
	defer cm.Unref(collID, 1)

	// add_function_field bumps the collection to V2 (stands in for the new function output field).
	v2Schema := mock_segcore.GenTestCollectionSchema("load_wins_v2", schemapb.DataType_Int64, false)
	v2Schema.Version = 2
	v2Schema.Fields = append(v2Schema.Fields, &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + int64(len(v2Schema.Fields)),
		Name:     "added_function_field",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})

	// Capture the served segcore version before the reopen so we can assert the applied
	// reservation lands at exactly served+1 for a single-version delta.
	_, servedSegcoreBefore := cm.Get(collID).SchemaAndSegcoreVersion()

	// The reopen reaches the delegator first. It reserves an APPLIED segcore version for the
	// reopened segments + refreshes index meta, but MUST NOT advance the served schema.
	s.Require().NoError(cm.ReserveAppliedSchemaForReopen(collID, v2Schema, mock_segcore.GenTestIndexMeta(collID, v2Schema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	}))
	// ReserveAppliedSchemaForReopen publishes a caller-visible Ref(1) like the load path; the
	// delegator immediately Unrefs it in syncCollectionIndexMeta, so mirror that here.
	cm.Unref(collID, 1)

	// The reopen did NOT advance the served schema: it is still V1.
	servedSchema, servedVersion := cm.Get(collID).SchemaAndVersion()
	s.Equal(uint64(1), servedVersion, "reopen must not advance the served logical schema version")
	s.Same(baseSchema, servedSchema, "reopen must not swap the served schema payload")
	_, servedSegcoreAfter := cm.Get(collID).SchemaAndSegcoreVersion()
	s.Equal(servedSegcoreBefore, servedSegcoreAfter, "reopen must not advance the served segcore version")

	// The APPLIED (segment-facing) snapshot IS the V2 schema, reserved at served+1.
	appliedSchema, appliedSegcore := cm.Get(collID).AppliedSchemaAndSegcoreVersion()
	s.Same(v2Schema, appliedSchema, "applied (segment-facing) schema must be V2 after a reopen")
	s.Equal(servedSegcoreBefore+1, appliedSegcore, "applied segcore version must be reserved at served+1")

	// The stream DDL (which rebuilds idfOracle / function runners) arrives afterwards at the same
	// V2 and MUST still be applied so the derived-state rebuild runs.
	s.True(
		ShouldUpdateCollectionSchema(cm.Get(collID), v2Schema, 100),
		"stream UpdateSchema(V2) after a load-wins V2 must still apply so derived state is rebuilt; a no-op here is the #50989/#51062 race",
	)

	// After the stream advances served to V2, the applied snapshot is no longer strictly ahead,
	// so AppliedSchemaAndSegcoreVersion falls back to served (reconcile with no extra reopen).
	s.Require().NoError(cm.UpdateSchema(collID, v2Schema, 100))
	servedSchema, servedVersion = cm.Get(collID).SchemaAndVersion()
	s.Equal(uint64(2), servedVersion, "stream UpdateSchema(V2) must advance the served logical schema version")
	s.Same(v2Schema, servedSchema)
	_, servedSegcoreReconciled := cm.Get(collID).SchemaAndSegcoreVersion()
	s.Equal(servedSegcoreBefore+1, servedSegcoreReconciled, "stream advance must reach the reserved applied segcore version")
	_, reconciledSegcore := cm.Get(collID).AppliedSchemaAndSegcoreVersion()
	s.Equal(servedSegcoreReconciled, reconciledSegcore, "applied must fall back to served once served catches up")
}

// TestReopenReservationIsIdempotentPerSchemaVersion is the Tier A drift/collision regression.
// On a co-located node the worker leg (services.go) and the delegator relay (syncCollectionIndexMeta)
// both reserve for the same reopen, and multiple pre-DDL segments / retries reopen the same
// version. The applied segcore version must stay pinned to what the stream will assign (served +
// logical distance), NOT drift ahead by one per reserve call. A drifting counter would, on a
// later delta, collide with the served segcore version and defeat segcore's
// ValidateSchemaCompatibility gate. This test fails under a naive max(served, applied)+1 reserve.
func (s *CollectionManagerSuite) TestReopenReservationIsIdempotentPerSchemaVersion() {
	cm := NewCollectionManager()
	const collID = 22

	baseSchema := mock_segcore.GenTestCollectionSchema("reopen_idem_v1", schemapb.DataType_Int64, false)
	baseSchema.Version = 1
	s.Require().NoError(cm.PutOrRef(collID, baseSchema, mock_segcore.GenTestIndexMeta(collID, baseSchema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	}))
	defer cm.Unref(collID, 1)

	_, servedSegcoreBefore := cm.Get(collID).SchemaAndSegcoreVersion()

	v2Schema := mock_segcore.GenTestCollectionSchema("reopen_idem_v2", schemapb.DataType_Int64, false)
	v2Schema.Version = 2

	reopenV2 := func() uint64 {
		s.Require().NoError(cm.ReserveAppliedSchemaForReopen(collID, v2Schema, mock_segcore.GenTestIndexMeta(collID, v2Schema), &querypb.LoadMetaInfo{
			LoadType:        querypb.LoadType_LoadCollection,
			SchemaBarrierTs: 100,
		}))
		cm.Unref(collID, 1)
		_, appliedSegcore := cm.Get(collID).AppliedSchemaAndSegcoreVersion()
		return appliedSegcore
	}

	first := reopenV2()
	second := reopenV2()
	third := reopenV2()

	s.Equal(servedSegcoreBefore+1, first, "first reopen of V2 reserves served+1")
	s.Equal(first, second, "a second reopen of the SAME V2 must reuse the reservation, not drift")
	s.Equal(first, third, "further reopens of the same V2 must not drift the applied segcore version")

	// No number of reopens may touch the served schema.
	_, servedVersion := cm.Get(collID).SchemaAndVersion()
	s.Equal(uint64(1), servedVersion, "reopens must not advance the served logical schema version")
	_, servedSegcoreAfter := cm.Get(collID).SchemaAndSegcoreVersion()
	s.Equal(servedSegcoreBefore, servedSegcoreAfter, "reopens must not advance the served segcore version")
}

// TestFullLoadStillAdvancesServedSchema guards against accidentally gating the full-load path
// behind the applied-vs-served split. A LoadScope_Full PutOrRef of a version-ahead schema (a
// genuinely post-DDL segment born at the new version) MUST still advance the served schema; only
// LoadScope_Reopen reserves applied-only.
func (s *CollectionManagerSuite) TestFullLoadStillAdvancesServedSchema() {
	cm := NewCollectionManager()
	const collID = 21

	baseSchema := mock_segcore.GenTestCollectionSchema("full_load_v1", schemapb.DataType_Int64, false)
	baseSchema.Version = 1
	s.Require().NoError(cm.PutOrRef(collID, baseSchema, mock_segcore.GenTestIndexMeta(collID, baseSchema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	}))
	defer cm.Unref(collID, 1)

	v2Schema := mock_segcore.GenTestCollectionSchema("full_load_v2", schemapb.DataType_Int64, false)
	v2Schema.Version = 2

	// A full load of a genuinely-V2 segment advances the served schema as before (PutOrRef).
	s.Require().NoError(cm.PutOrRef(collID, v2Schema, mock_segcore.GenTestIndexMeta(collID, v2Schema), &querypb.LoadMetaInfo{
		LoadType:        querypb.LoadType_LoadCollection,
		SchemaBarrierTs: 100,
	}))
	cm.Unref(collID, 1)

	servedSchema, servedVersion := cm.Get(collID).SchemaAndVersion()
	s.Equal(uint64(2), servedVersion, "full load must still advance the served logical schema version")
	s.Same(v2Schema, servedSchema, "full load must swap the served schema payload to V2")
	s.False(
		ShouldUpdateCollectionSchema(cm.Get(collID), v2Schema, 100),
		"full load already advanced served, so a same-version stream UpdateSchema is a no-op",
	)
}

func TestCollectionManager(t *testing.T) {
	suite.Run(t, new(CollectionManagerSuite))
}
