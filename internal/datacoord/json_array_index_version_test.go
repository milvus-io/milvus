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

package datacoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func jsonArrayVersionManager(t *testing.T, version int32) IndexEngineVersionManager {
	t.Helper()
	manager := newIndexEngineVersionManager()
	manager.AddNode(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{
		ServerID: 1,
		ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{
			CurrentIndexVersion: version,
			MaximumIndexVersion: common.MaximumScalarIndexEngineVersion,
		},
	}})
	require.Equal(t, version, manager.ResolveScalarIndexVersion())
	return manager
}

func resetJSONVersionTargets(t *testing.T) {
	t.Helper()
	paramtable.Init()
	target := Params.DataCoordCfg.TargetScalarIndexVersion.GetValue()
	force := Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetValue()
	require.NoError(t, Params.Save(Params.DataCoordCfg.TargetScalarIndexVersion.Key, "-1"))
	require.NoError(t, Params.Save(Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, "false"))
	t.Cleanup(func() {
		require.NoError(t, Params.Save(Params.DataCoordCfg.TargetScalarIndexVersion.Key, target))
		require.NoError(t, Params.Save(Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.Key, force))
	})
}

func TestCreateIndexArraySortEngineVersion(t *testing.T) {
	resetJSONVersionTargets(t)
	initStreamingSystem(t)
	for _, indexType := range []string{indexparamcheck.IndexHybrid, indexparamcheck.IndexSTLSORT} {
		for _, cast := range []string{"", "ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
			for _, version := range []int32{0, 5, 6} {
				for _, auto := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/v%d/auto=%t", indexType, cast, version, auto), func(t *testing.T) {
						field := &schemapb.FieldSchema{FieldID: 100, Name: "payload", DataType: schemapb.DataType_JSON}
						params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}}
						if cast == "" {
							field.DataType = schemapb.DataType_Array
							field.ElementType = schemapb.DataType_Double
						} else {
							params = append(params, &commonpb.KeyValuePair{Key: common.JSONCastTypeKey, Value: cast},
								&commonpb.KeyValuePair{Key: common.JSONPathKey, Value: `payload["a"]`})
						}
						coordinator := mocks.NewMixCoord(t)
						coordinator.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(
							&milvuspb.DescribeCollectionResponse{
								Status: merr.Success(), CollectionName: "collection", DbName: "default",
								Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}},
							}, nil).Twice()
						alloc := allocator.NewMockAllocator(t)
						incompatible := version < 6 && (cast != "" || indexType == indexparamcheck.IndexSTLSORT)
						blocked := incompatible && !auto
						if !blocked {
							// Stop after admission, before metadata or broadcasting.
							alloc.EXPECT().AllocID(mock.Anything).Return(int64(0),
								merr.WrapErrServiceInternalMsg("passed ARRAY capability gate")).Once()
						}
						server := &Server{
							broker: broker.NewCoordinatorBroker(coordinator), allocator: alloc,
							indexEngineVersionManager: jsonArrayVersionManager(t, version),
						}
						server.stateCode.Store(commonpb.StateCode_Healthy)
						request := &indexpb.CreateIndexRequest{
							CollectionID: 1, FieldID: 100, IndexName: "array_index", IsAutoIndex: auto, IndexParams: params,
						}
						status, err := server.CreateIndex(context.Background(), request)
						require.NoError(t, err)
						if blocked {
							require.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
							require.True(t, status.GetRetriable())
							alloc.AssertNotCalled(t, "AllocID", mock.Anything)
						} else {
							require.Contains(t, status.GetReason(), "passed ARRAY capability gate")
						}
						expectedType := indexType
						if auto && incompatible {
							expectedType = indexparamcheck.IndexINVERTED
						}
						require.Equal(t, expectedType, common.GetIndexType(request.GetIndexParams()))
					})
				}
			}
		}
	}
}

func TestRestoreJSONArrayEngineVersion(t *testing.T) {
	resetJSONVersionTargets(t)
	for _, indexType := range []string{indexparamcheck.IndexHybrid, indexparamcheck.IndexSTLSORT} {
		for _, cast := range []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
			for _, version := range []int32{0, 5, 6} {
				t.Run(fmt.Sprintf("%s/%s/v%d", indexType, cast, version), func(t *testing.T) {
					manager := &snapshotManager{indexEngineVersionManager: jsonArrayVersionManager(t, version)}
					err := manager.checkJSONPathIndexVersion(&model.Index{IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: indexType},
						{Key: common.JSONCastTypeKey, Value: cast},
						{Key: common.JSONPathKey, Value: "/a"},
					}})
					if version < 6 {
						require.ErrorIs(t, err, merr.ErrServiceNotReady)
						require.True(t, merr.Status(err).GetRetriable())
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
	for _, tc := range []struct {
		name, indexType, cast string
		version               int32
	}{
		{"scalar_hybrid_v4", indexparamcheck.IndexHybrid, "DOUBLE", 4},
		{"scalar_sort_v4", indexparamcheck.IndexSTLSORT, "DOUBLE", 4},
		{"scalar_bitmap_v4", indexparamcheck.IndexBitmap, "BOOL", 4},
		{"array_inverted_v3", indexparamcheck.IndexINVERTED, "ARRAY_DOUBLE", 3},
		{"full_json_inverted_v3", indexparamcheck.IndexINVERTED, "JSON", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := &snapshotManager{indexEngineVersionManager: jsonArrayVersionManager(t, tc.version)}
			require.NoError(t, manager.checkJSONPathIndexVersion(&model.Index{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: tc.indexType},
				{Key: common.JSONCastTypeKey, Value: tc.cast},
				{Key: common.JSONPathKey, Value: "/a"},
			}}))
		})
	}
}

func TestRestoreArraySortEngineVersion(t *testing.T) {
	resetJSONVersionTargets(t)
	for _, tc := range []struct {
		name, indexType, cast string
		field                 *schemapb.FieldSchema
		version               int32
		blocked               bool
	}{
		{"ordinary", indexparamcheck.IndexSTLSORT, "", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}, 5, true},
		{"ordinary_v6", indexparamcheck.IndexSTLSORT, "", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}, 6, false},
		{"ordinary_no_readers", indexparamcheck.IndexSTLSORT, "", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Double}, 0, true},
		{"json_sort", indexparamcheck.IndexSTLSORT, "ARRAY_DOUBLE", &schemapb.FieldSchema{Name: "payload", DataType: schemapb.DataType_JSON}, 5, true},
		{"json_sort_v6", indexparamcheck.IndexSTLSORT, "ARRAY_DOUBLE", &schemapb.FieldSchema{Name: "payload", DataType: schemapb.DataType_JSON}, 6, false},
		{"json_hybrid", indexparamcheck.IndexHybrid, "ARRAY_VARCHAR", &schemapb.FieldSchema{Name: "payload", DataType: schemapb.DataType_JSON}, 5, true},
		{"json_hybrid_v6", indexparamcheck.IndexHybrid, "ARRAY_VARCHAR", &schemapb.FieldSchema{Name: "payload", DataType: schemapb.DataType_JSON}, 6, false},
		{"existing_nested", indexparamcheck.IndexSTLSORT, "", &schemapb.FieldSchema{Name: "items[value]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, 4, false},
		{"existing_scalar", indexparamcheck.IndexSTLSORT, "", &schemapb.FieldSchema{Name: "value", DataType: schemapb.DataType_Int64}, 5, false},
		{"existing_array_hybrid", indexparamcheck.IndexHybrid, "", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.field.FieldID = 100
			params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: tc.indexType}}
			if tc.cast != "" {
				params = append(params, &commonpb.KeyValuePair{Key: common.JSONCastTypeKey, Value: tc.cast},
					&commonpb.KeyValuePair{Key: common.JSONPathKey, Value: "/a"})
			}
			mockBroker := broker.NewMockBroker(t)
			mockBroker.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(
				&milvuspb.DescribeCollectionResponse{DbName: "default", Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{tc.field}}}, nil).Once()
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()
			sm := &snapshotManager{broker: mockBroker, allocator: alloc, indexEngineVersionManager: jsonArrayVersionManager(t, tc.version)}
			broadcastStarted := false
			err := sm.RestoreIndexes(context.Background(), &snapshotstorage.SnapshotData{Indexes: []*indexpb.IndexInfo{{
				FieldID: 100, IndexName: "array_index", IndexParams: params,
			}}}, 1, func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
				broadcastStarted = true
				return nil, merr.WrapErrServiceInternalMsg("passed ARRAY capability gate")
			}, "snapshot")
			if tc.blocked {
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
				require.True(t, merr.Status(err).GetRetriable())
				require.False(t, broadcastStarted)
			} else {
				require.True(t, broadcastStarted)
				require.ErrorContains(t, err, "passed ARRAY capability gate")
			}
		})
	}
}
