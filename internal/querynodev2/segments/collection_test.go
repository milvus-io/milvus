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
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
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

func (s *CollectionManagerSuite) TestLoadSchemaSnapshot() {
	schema := mock_segcore.GenTestCollectionSchema("snapshot", schemapb.DataType_Int64, false)
	collection := NewCollectionWithoutSegcoreForTest(10, schema)
	collection.setSchema(schema, 7, 100, 101)
	fieldID := schema.GetFields()[0].GetFieldID()
	collection.loadFields = typeutil.NewSet(fieldID)

	loadedSchema, version, fields := collection.LoadSchemaSnapshot()
	s.Same(schema, loadedSchema)
	s.Equal(uint64(101), version, "reopen must use the native schema version, not the logical version")
	s.Equal([]int64{fieldID}, fields)
	fields[0] = 999
	_, _, fields = collection.LoadSchemaSnapshot()
	s.Equal([]int64{fieldID}, fields, "the returned slice must not alias collection state")

	allFields := lo.Map(schema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 { return field.GetFieldID() })
	collection.loadFieldsDefault = true
	_, _, fields = collection.LoadSchemaSnapshot()
	s.ElementsMatch(allFields, fields, "default-all must resolve against the current schema")

	collection.loadFieldsDefault = false
	collection.loadFields = nil
	_, _, fields = collection.LoadSchemaSnapshot()
	s.ElementsMatch(allFields, fields, "legacy collections without a stored hint must resolve all fields")
}

func (s *CollectionManagerSuite) TestLoadSchemaSnapshotKeepsSchemaAndFieldsInSameEpoch() {
	schema := &schemapb.CollectionSchema{Name: "snapshot_0"}
	collection := NewCollectionWithoutSegcoreForTest(10, schema)
	collection.setSchema(schema, 0, 0, 100)
	collection.loadFields = typeutil.NewSet(int64(100))

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
			schema, version, fields := collection.LoadSchemaSnapshot()
			if schema.GetName() != fmt.Sprintf("snapshot_%d", version-100) || len(fields) != 1 || fields[0] != int64(version) {
				select {
				case errCh <- fmt.Sprintf("mixed load snapshot: schema=%s, version=%d, fields=%v", schema.GetName(), version, fields):
				default:
				}
				return
			}
		}
	}()

	for i := 1; i <= 1000; i++ {
		collection.lockSchemaTransitionForUpdate()
		collection.setSchema(&schemapb.CollectionSchema{Name: fmt.Sprintf("snapshot_%d", i)}, uint64(i), uint64(i), uint64(100+i))
		// Exercise the interval between schema publication and the later field
		// hint update in applyLoadUpdate; readers must wait for both to finish.
		runtime.Gosched()
		collection.mu.Lock()
		collection.loadFields = typeutil.NewSet(int64(100 + i))
		collection.mu.Unlock()
		collection.unlockSchemaTransitionForUpdate()
	}
	close(stop)
	wg.Wait()
	select {
	case msg := <-errCh:
		s.Fail(msg)
	default:
	}
	schema, version, fields := collection.LoadSchemaSnapshot()
	s.Equal("snapshot_1000", schema.GetName())
	s.Equal(uint64(1100), version)
	s.Equal([]int64{1100}, fields)
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

func (s *CollectionManagerSuite) TestPutOrRefPublishesLoadUpdateBeforeNativeReaders() {
	cm := NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("atomic_load_update", schemapb.DataType_Int64, false)
	initialFields := []int64{schema.GetFields()[0].GetFieldID()}
	initialIndexMeta := mock_segcore.GenTestIndexMeta(10, schema)
	s.Require().NoError(cm.PutOrRef(10, schema, initialIndexMeta, &querypb.LoadMetaInfo{
		LoadFields: initialFields,
	}))
	defer cm.Unref(10, 1)
	coll := cm.Get(10)

	newSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	newSchema.Version++
	newSchema.Fields = append(newSchema.Fields, &schemapb.FieldSchema{
		FieldID:  580,
		Name:     "new_warmup_field",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})
	newFields := append(append([]int64(nil), initialFields...), 580)
	newIndexMeta := proto.Clone(initialIndexMeta).(*segcorepb.CollectionIndexMeta)
	newIndexMeta.MaxIndexRowCount++
	retrieveReq := s.newSimpleRetrieveRequest(coll)

	// Stop after the native schema has changed and its Go snapshot has been
	// published, but before the new index metadata and warmup hint are applied.
	// The old implementation released mu at this exact point, allowing native
	// readers to capture the new schema together with the previous hint.
	schemaPublished := make(chan struct{})
	continueUpdate := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpdate := func() { releaseOnce.Do(func() { close(continueUpdate) }) }
	var setSchemaOrigin func(*Collection, *schemapb.CollectionSchema, uint64, uint64, uint64)
	schemaPatch := mockey.Mock((*Collection).setSchema).To(func(c *Collection, schema *schemapb.CollectionSchema, logicalVersion, barrierTs, nativeVersion uint64) {
		setSchemaOrigin(c, schema, logicalVersion, barrierTs, nativeVersion)
		close(schemaPublished)
		<-continueUpdate
	}).Origin(&setSchemaOrigin).Build()
	defer schemaPatch.UnPatch()

	hintPublished := make(chan struct{})
	nativeLoadFields := append([]int64(nil), initialFields...)
	var loadFieldsOrigin func(*segcore.CCollection, []int64) error
	hintPatch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).To(func(c *segcore.CCollection, fields []int64) error {
		if err := loadFieldsOrigin(c, fields); err != nil {
			return err
		}
		nativeLoadFields = append([]int64(nil), fields...)
		close(hintPublished)
		return nil
	}).Origin(&loadFieldsOrigin).Build()
	defer hintPatch.UnPatch()

	type nativeReadSnapshot struct {
		reader           string
		schemaVersion    uint64
		nativeVersion    uint64
		indexMeta        *segcorepb.CollectionIndexMeta
		loadFields       []int64
		nativeLoadFields []int64
		hintPublished    bool
	}
	observed := make(chan nativeReadSnapshot, 3)
	capture := func(reader string) {
		_, version := coll.SchemaAndVersion()
		_, nativeVersion := coll.SchemaAndSegcoreVersion()
		var hintReady bool
		select {
		case <-hintPublished:
			hintReady = true
		default:
		}
		observed <- nativeReadSnapshot{
			reader:           reader,
			schemaVersion:    version,
			nativeVersion:    nativeVersion,
			indexMeta:        coll.ccollection.IndexMeta(),
			loadFields:       coll.loadFields.Collect(),
			nativeLoadFields: append([]int64(nil), nativeLoadFields...),
			hintPublished:    hintReady,
		}
	}
	segmentPatch := mockey.Mock(segcore.CreateCSegment).To(func(*segcore.CreateCSegmentRequest) (segcore.CSegment, error) {
		capture("CreateCSegment")
		return nil, nil
	}).Build()
	defer segmentPatch.UnPatch()
	searchPatch := mockey.Mock(segcore.NewSearchRequest).To(func(*segcore.CCollection, *querypb.SearchRequest, []byte) (*segcore.SearchRequest, error) {
		capture("NewSearchRequest")
		return nil, nil
	}).Build()
	defer searchPatch.UnPatch()
	retrievePatch := mockey.Mock(segcore.NewRetrievePlan).To(func(*segcore.CCollection, []byte, typeutil.Timestamp, int64, commonpb.ConsistencyLevel, typeutil.Timestamp, typeutil.Timestamp) (*segcore.RetrievePlan, error) {
		capture("NewRetrievePlan")
		return nil, nil
	}).Build()
	defer retrievePatch.UnPatch()

	updateDone := make(chan error, 1)
	updateFinished := make(chan struct{})
	go func() {
		defer close(updateFinished)
		err := cm.PutOrRef(10, newSchema, newIndexMeta, &querypb.LoadMetaInfo{
			LoadFields:      newFields,
			SchemaBarrierTs: 100,
		})
		if err == nil {
			cm.Unref(10, 1)
		}
		updateDone <- err
	}()
	var readers sync.WaitGroup
	defer func() {
		releaseUpdate()
		select {
		case <-updateFinished:
		case <-time.After(5 * time.Second):
			s.T().Fatal("load update did not finish")
		}
		readersFinished := make(chan struct{})
		go func() {
			readers.Wait()
			close(readersFinished)
		}()
		select {
		case <-readersFinished:
		case <-time.After(5 * time.Second):
			s.T().Fatal("native readers did not finish")
		}
	}()

	select {
	case <-schemaPublished:
	case <-time.After(5 * time.Second):
		s.T().Fatal("load update did not reach the schema publication point")
	}
	// TryRLock makes the exclusion assertion deterministic: it fails the old
	// split publication even if the scheduler has not run a reader goroutine.
	readLockAcquired := coll.mu.TryRLock()
	if readLockAcquired {
		coll.mu.RUnlock()
	}
	s.Require().False(readLockAcquired, "native readers must be excluded until schema, index metadata, and hints are all published")

	readerStarted := make(chan struct{}, 3)
	readerDone := make(chan error, 3)
	for _, read := range []func() error{
		func() error {
			_, err := coll.CreateCSegment(&segcore.CreateCSegmentRequest{SegmentID: 1, SegmentType: SegmentTypeSealed})
			return err
		},
		func() error {
			_, err := coll.NewSearchRequest(&querypb.SearchRequest{}, nil)
			return err
		},
		func() error {
			_, err := coll.NewRetrievePlan(retrieveReq)
			return err
		},
	} {
		readers.Add(1)
		go func() {
			defer readers.Done()
			readerStarted <- struct{}{}
			readerDone <- read()
		}()
	}
	for range 3 {
		<-readerStarted
	}
	select {
	case snapshot := <-observed:
		s.T().Fatalf("%s entered native code before warmup hints were published", snapshot.reader)
	default:
	}

	releaseUpdate()
	select {
	case err := <-updateDone:
		s.Require().NoError(err)
	case <-time.After(5 * time.Second):
		s.T().Fatal("load update did not complete after publication resumed")
	}
	for range 3 {
		select {
		case err := <-readerDone:
			s.Require().NoError(err)
		case <-time.After(5 * time.Second):
			s.T().Fatal("native reader did not resume after load update")
		}
		snapshot := <-observed
		s.Equal(uint64(newSchema.GetVersion()), snapshot.schemaVersion, snapshot.reader)
		s.Equal(uint64(1), snapshot.nativeVersion, snapshot.reader)
		s.Same(newIndexMeta, snapshot.indexMeta, snapshot.reader)
		s.ElementsMatch(newFields, snapshot.loadFields, snapshot.reader)
		s.ElementsMatch(newFields, snapshot.nativeLoadFields, snapshot.reader)
		s.True(snapshot.hintPublished, "%s must observe the updated native hint", snapshot.reader)
	}
}

func (s *CollectionManagerSuite) TestPutOrRefUpdatesLoadFields() {
	cm := NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("load_fields", schemapb.DataType_Int64, false)
	allFields := lo.Map(schema.GetFields(), func(field *schemapb.FieldSchema, _ int) int64 { return field.GetFieldID() })
	s.Require().GreaterOrEqual(len(allFields), 2)
	initialFields := allFields[:1]

	var nativeFields []int64
	var nativeCalls int
	var origin func(*segcore.CCollection, []int64) error
	patch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).To(func(c *segcore.CCollection, fields []int64) error {
		nativeFields = append([]int64(nil), fields...)
		nativeCalls++
		return origin(c, fields)
	}).Origin(&origin).Build()
	defer patch.UnPatch()

	s.Require().NoError(cm.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
		LoadFields:      initialFields,
		SchemaBarrierTs: 100,
	}))
	defer cm.Unref(10, 1)
	coll := cm.Get(10)
	s.ElementsMatch(initialFields, coll.loadFields.Collect())
	s.ElementsMatch(initialFields, nativeFields)

	// The schema and its barrier do not change when the warmup hint changes.
	for _, fields := range [][]int64{allFields, allFields[1:], initialFields} {
		s.Require().NoError(cm.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
			LoadFields:      fields,
			SchemaBarrierTs: 100,
		}))
		cm.Unref(10, 1)
		s.ElementsMatch(fields, coll.loadFields.Collect())
		s.ElementsMatch(fields, nativeFields)
	}
	s.Equal(4, nativeCalls)
	s.Require().NoError(cm.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
		LoadFields:      initialFields,
		SchemaBarrierTs: 100,
	}))
	cm.Unref(10, 1)
	s.Equal(4, nativeCalls, "repeated identical hints must not copy the native schema")

	newSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	newSchema.Version++
	newSchema.Fields = append(newSchema.Fields, &schemapb.FieldSchema{
		FieldID:  580,
		Name:     "new_warmup_field",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})
	newFields := append(append([]int64(nil), initialFields...), 580)
	s.Require().NoError(cm.PutOrRef(10, newSchema, nil, &querypb.LoadMetaInfo{
		LoadFields:      newFields,
		SchemaBarrierTs: 101,
	}))
	cm.Unref(10, 1)
	s.ElementsMatch(newFields, coll.loadFields.Collect())
	s.ElementsMatch(newFields, nativeFields)
	s.Equal(uint64(newSchema.GetVersion()), coll.SchemaVersion())

	// Schema-only and legacy loads must leave an existing subset intact.
	s.Require().NoError(cm.PutOrRef(10, newSchema, nil, nil))
	cm.Unref(10, 1)
	s.Require().NoError(cm.PutOrRef(10, newSchema, nil, &querypb.LoadMetaInfo{SchemaBarrierTs: 102}))
	cm.Unref(10, 1)
	s.Equal(5, nativeCalls)
	s.ElementsMatch(newFields, coll.loadFields.Collect())

	// A present empty list restores native default-all rather than a frozen
	// list of current IDs, so subsequently added fields use that default too.
	s.Require().NoError(cm.PutOrRef(10, newSchema, nil, &querypb.LoadMetaInfo{
		LoadFields:      []int64{},
		SchemaBarrierTs: 102,
	}))
	cm.Unref(10, 1)
	s.Equal(6, nativeCalls)
	s.Empty(nativeFields)
	s.ElementsMatch(append(append([]int64(nil), allFields...), 580), coll.loadFields.Collect())
	s.Require().NoError(cm.PutOrRef(10, newSchema, nil, &querypb.LoadMetaInfo{
		LoadFields:      []int64{},
		SchemaBarrierTs: 102,
	}))
	cm.Unref(10, 1)
	s.Equal(6, nativeCalls, "repeated default-all hints must not copy the native schema")

	newestSchema := proto.Clone(newSchema).(*schemapb.CollectionSchema)
	newestSchema.Version++
	newestSchema.Fields = append(newestSchema.Fields, &schemapb.FieldSchema{
		FieldID:  581,
		Name:     "future_warmup_field",
		DataType: schemapb.DataType_Bool,
		Nullable: true,
	})
	s.Require().NoError(cm.PutOrRef(10, newestSchema, nil, &querypb.LoadMetaInfo{SchemaBarrierTs: 103}))
	cm.Unref(10, 1)
	s.Equal(6, nativeCalls, "schema-only refreshes inherit the native default-all hint")
	s.True(coll.loadFieldsDefault)
	s.ElementsMatch(append(append([]int64(nil), allFields...), 580, 581), coll.loadFields.Collect())

	// First loads with either nil or empty hints retain the legacy default.
	for _, fields := range [][]int64{nil, {}} {
		freshManager := NewCollectionManager()
		var createdFields []int64
		var createOrigin func(*segcore.CreateCCollectionRequest) (*segcore.CCollection, error)
		createPatch := mockey.Mock(segcore.CreateCCollection).To(func(req *segcore.CreateCCollectionRequest) (*segcore.CCollection, error) {
			createdFields = req.LoadFieldList
			return createOrigin(req)
		}).Origin(&createOrigin).Build()
		err := freshManager.PutOrRef(11, schema, nil, &querypb.LoadMetaInfo{LoadFields: fields})
		createPatch.UnPatch()
		s.Require().NoError(err)
		fresh := freshManager.Get(11)
		s.Empty(createdFields, "native creation must retain default-all, not freeze current field IDs")
		s.True(fresh.loadFieldsDefault)
		s.ElementsMatch(allFields, fresh.loadFields.Collect())
		s.Require().NoError(freshManager.UpdateSchema(11, newSchema, 101))
		_, _, snapshotFields := fresh.LoadSchemaSnapshot()
		s.True(fresh.loadFieldsDefault)
		s.ElementsMatch(append(append([]int64(nil), allFields...), 580), snapshotFields)
		freshManager.Unref(11, 1)
	}
}

func (s *CollectionManagerSuite) TestPutOrRefLoadFieldsRejectsStaleSnapshots() {
	cm := NewCollectionManager()
	schema := mock_segcore.GenTestCollectionSchema("load_fields", schemapb.DataType_Int64, false)
	schema.Version = 3
	initialFields := []int64{schema.GetFields()[0].GetFieldID()}
	s.Require().NoError(cm.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
		LoadFields:      initialFields,
		SchemaBarrierTs: 100,
	}))
	defer cm.Unref(10, 1)
	coll := cm.Get(10)

	var nativeCalls int
	var origin func(*segcore.CCollection, []int64) error
	patch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).To(func(c *segcore.CCollection, fields []int64) error {
		nativeCalls++
		return origin(c, fields)
	}).Origin(&origin).Build()
	defer patch.UnPatch()
	expandedFields := []int64{schema.GetFields()[0].GetFieldID(), schema.GetFields()[1].GetFieldID()}
	s.Require().NoError(cm.PutOrRef(10, schema, nil, &querypb.LoadMetaInfo{
		LoadFields:      expandedFields,
		SchemaBarrierTs: 200,
	}))
	cm.Unref(10, 1)

	olderSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	olderSchema.Version--
	for _, stale := range []struct {
		schema  *schemapb.CollectionSchema
		barrier uint64
	}{
		{schema, 100},
		{olderSchema, 300},
	} {
		s.Require().NoError(cm.PutOrRef(10, stale.schema, nil, &querypb.LoadMetaInfo{
			LoadFields:      initialFields,
			SchemaBarrierTs: stale.barrier,
		}))
		cm.Unref(10, 1)
		s.ElementsMatch(expandedFields, coll.loadFields.Collect())
	}
	s.Equal(1, nativeCalls, "stale messages must not modify native warmup hints")

	// Preserve the existing two-domain ordering rule: a newer structural
	// schema is accepted even when its barrier is below the current barrier.
	newerSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	newerSchema.Version++
	s.Require().NoError(cm.PutOrRef(10, newerSchema, nil, &querypb.LoadMetaInfo{
		LoadFields:      initialFields,
		SchemaBarrierTs: 150,
	}))
	cm.Unref(10, 1)
	s.Equal(2, nativeCalls)
	s.ElementsMatch(initialFields, coll.loadFields.Collect())
	_, version, barrier := coll.SchemaSnapshot()
	s.Equal(uint64(4), version)
	s.Equal(uint64(200), barrier)
}

func (s *CollectionManagerSuite) TestPutOrRefLoadFieldsFailureDoesNotPublishRef() {
	coll := s.cm.Get(1)
	initialFields := coll.loadFields.Collect()
	errNative := errors.New("update load fields failed")
	patch := mockey.Mock((*segcore.CCollection).UpdateLoadFields).Return(errNative).Build()
	defer patch.UnPatch()

	err := s.cm.PutOrRef(1, coll.Schema(), nil, &querypb.LoadMetaInfo{LoadFields: initialFields[:1]})
	s.ErrorIs(err, errNative)
	s.Equal(uint32(1), coll.refCount.Load(), "failed updates must release the temporary lease without publishing a caller ref")
	s.ElementsMatch(initialFields, coll.loadFields.Collect())
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

func (s *CollectionManagerSuite) TestPutOrRefLoadFieldsLeaseKeepsCollectionAliveWhileWaiting() {
	coll := s.cm.Get(1)
	fields := []int64{coll.Schema().GetFields()[0].GetFieldID()}
	releaseReader := holdInsertSchemaTransition(s.T(), coll)
	defer releaseReader()
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- s.cm.PutOrRef(coll.ID(), coll.Schema(), nil, &querypb.LoadMetaInfo{LoadFields: fields})
	}()
	waitForSchemaTransitionWriter(s.T(), coll)

	unrefDone := make(chan bool, 1)
	go func() {
		unrefDone <- s.cm.Unref(coll.ID(), 1)
	}()
	select {
	case released := <-unrefDone:
		s.False(released, "the load-update lease must retain the collection")
	case <-time.After(5 * time.Second):
		s.T().Fatal("Unref blocked while load fields waited for an insert transition reader")
	}
	s.Same(coll, s.cm.Get(coll.ID()))
	s.Equal(uint32(1), coll.refCount.Load(), "only the temporary lease may be visible while the update waits")

	releaseReader()
	s.Require().NoError(<-updateDone)
	s.Same(coll, s.cm.Get(coll.ID()), "successful PutOrRef must publish the caller ref before dropping its lease")
	s.ElementsMatch(fields, coll.loadFields.Collect())
	s.Equal(uint32(1), coll.refCount.Load())
	s.True(s.cm.Unref(coll.ID(), 1))
	s.Nil(s.cm.Get(coll.ID()))
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
	s.Error(coll.updateLoadFields(nil))
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

func (s *CollectionManagerSuite) TestGetIndexType() {
	collection := s.cm.Get(1)
	s.Require().NotNil(collection)
	s.Require().NotEmpty(collection.GetCCollection().IndexMeta().GetIndexMetas())

	indexMeta := collection.GetCCollection().IndexMeta().GetIndexMetas()[0]
	s.Equal(mock_segcore.IndexFaissIVFFlat, collection.GetIndexType(indexMeta.GetFieldID()))
	s.Empty(collection.GetIndexType(-1))
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
