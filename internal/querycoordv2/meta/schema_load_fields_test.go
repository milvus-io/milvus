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

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func schemaLoadFieldsFixture(t *testing.T, fields []int64) (*CollectionManager, *catalogmocks.QueryCoordCatalog) {
	t.Helper()
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	mgr := NewCollectionManager(catalog)
	require.NoError(t, mgr.PutCollectionWithoutSave(t.Context(), &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 10, LoadFields: fields, ReplicaNumber: 2, Status: querypb.LoadStatus_Loaded, LoadType: querypb.LoadType_LoadPartition, FieldIndexID: map[int64]int64{101: 900}},
		LoadPercentage:     100, CreatedAt: time.Unix(1, 0), refreshNotifier: make(chan struct{}),
	}))
	return mgr, catalog
}

func TestUpdateLoadFieldsForSchemaAddsOnlyNewDefaultFields(t *testing.T) {
	previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102}}}
	target := proto.Clone(previous).(*schemapb.CollectionSchema)
	target.Fields = append(target.Fields,
		&schemapb.FieldSchema{FieldID: 0},
		&schemapb.FieldSchema{FieldID: 103},
		&schemapb.FieldSchema{FieldID: 104, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}},
		&schemapb.FieldSchema{FieldID: 105, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "invalid"}}},
		&schemapb.FieldSchema{FieldID: 109, IsDynamic: true})
	target.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 106, Fields: []*schemapb.FieldSchema{{FieldID: 107}, {FieldID: 108, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}}}}}
	mgr, catalog := schemaLoadFieldsFixture(t, []int64{100, 101})
	before := mgr.GetCollection(t.Context(), 10)
	var saved *querypb.CollectionLoadInfo
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Run(func(_ context.Context, info *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) {
		saved = proto.Clone(info).(*querypb.CollectionLoadInfo)
	}).Return(nil).Once()
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, previous, target))
	after := mgr.GetCollection(t.Context(), 10)
	require.Equal(t, []int64{100, 101, 103, 105, 109, 107}, after.GetLoadFields())
	require.Equal(t, []int64{100, 101}, before.GetLoadFields(), "do not mutate a reader's prior snapshot")
	require.Equal(t, before.LoadPercentage, after.LoadPercentage)
	require.Equal(t, before.CreatedAt, after.CreatedAt)
	require.Equal(t, before.refreshNotifier, after.refreshNotifier)
	require.Equal(t, before.GetFieldIndexID(), after.GetFieldIndexID())
	require.Equal(t, before.GetReplicaNumber(), after.GetReplicaNumber())
	require.Equal(t, before.GetStatus(), after.GetStatus())
	require.Equal(t, before.GetLoadType(), after.GetLoadType())
	// Replaying the same durable ALTER must not write again or duplicate fields.
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, previous, target))
	// A properties-only schema callback must not backfill excluded/stale fields.
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, target, target))
	data, err := proto.Marshal(saved)
	require.NoError(t, err)
	recovered := &querypb.CollectionLoadInfo{}
	require.NoError(t, proto.Unmarshal(data, recovered))
	require.Equal(t, after.GetLoadFields(), recovered.GetLoadFields(), "new fields survive persisted protobuf roundtrip")
}

func TestUpdateLoadFieldsForSchemaRetriesFailedSave(t *testing.T) {
	mgr, catalog := schemaLoadFieldsFixture(t, []int64{100})
	previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}}}
	target := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	failed := merr.WrapErrServiceUnavailable("injected catalog failure")
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(failed).Once()
	require.ErrorIs(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, previous, target), failed)
	require.Equal(t, []int64{100}, mgr.GetLoadFields(t.Context(), 10))
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, previous, target))
	require.Equal(t, []int64{100, 101}, mgr.GetLoadFields(t.Context(), 10))
}

func TestUpdateLoadFieldsForSchemaDoesNotLoadReleasedCollection(t *testing.T) {
	mgr, _ := schemaLoadFieldsFixture(t, []int64{100})
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 99, nil, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101}}}))
	require.Nil(t, mgr.GetCollection(t.Context(), 99))
}

func TestUpdateLoadFieldsForSchemaPreservesEmptyMeaning(t *testing.T) {
	mgr, catalog := schemaLoadFieldsFixture(t, nil)
	previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}}}
	target := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	_ = catalog // Legacy empty means all fields, so no write is required.
	require.NoError(t, mgr.UpdateLoadFieldsForSchema(t.Context(), 10, previous, target))
	require.Equal(t, []int64(nil), mgr.GetLoadFields(t.Context(), 10))
}
