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

func setJSONArrayIndexGate(t *testing.T, enabled bool) {
	t.Helper()
	previous := Params.DataCoordCfg.EnableArraySortIndex.SwapTempValue(fmt.Sprint(enabled))
	t.Cleanup(func() { Params.DataCoordCfg.EnableArraySortIndex.SwapTempValue(previous) })
}

func TestCreateIndexJSONArraySoftwareGate(t *testing.T) {
	resetJSONVersionTargets(t)
	initStreamingSystem(t)
	for _, indexType := range []string{indexparamcheck.IndexHybrid, indexparamcheck.IndexSTLSORT} {
		casts := []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"}
		if indexType == indexparamcheck.IndexSTLSORT {
			casts = append(casts, "") // Ordinary ARRAY has no JSON cast parameters.
		}
		for _, cast := range casts {
			for _, tc := range []struct {
				name    string
				enabled bool
				version int32
			}{
				{"disabled", false, common.CurrentScalarIndexEngineVersion},
				{"enabled", true, common.CurrentScalarIndexEngineVersion},
				{"packed_v3", true, 3},
				{"legacy_v2", true, 2},
			} {
				for _, auto := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/auto=%t", indexType, cast, tc.name, auto), func(t *testing.T) {
						setJSONArrayIndexGate(t, tc.enabled)
						field := &schemapb.FieldSchema{FieldID: 100, Name: "payload", DataType: schemapb.DataType_JSON}
						params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}}
						if cast == "" {
							field.DataType = schemapb.DataType_Array
							field.ElementType = schemapb.DataType_Int64
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
						incompatible := !tc.enabled || tc.version < 3
						blocked := incompatible && !auto
						if !blocked {
							// Stop after admission, before metadata or broadcasting.
							alloc.EXPECT().AllocID(mock.Anything).Return(int64(0),
								merr.WrapErrServiceInternalMsg("passed JSON software gate")).Once()
						}
						server := &Server{
							broker:                    broker.NewCoordinatorBroker(coordinator),
							allocator:                 alloc,
							indexEngineVersionManager: jsonArrayVersionManager(t, tc.version),
						}
						server.stateCode.Store(commonpb.StateCode_Healthy)
						request := &indexpb.CreateIndexRequest{
							CollectionID: 1, FieldID: 100, IndexName: "json_array",
							IsAutoIndex: auto,
							IndexParams: params,
						}
						status, err := server.CreateIndex(context.Background(), request)
						require.NoError(t, err)
						if blocked {
							if !tc.enabled {
								require.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
							} else {
								require.ErrorIs(t, merr.Error(status), merr.ErrParameterInvalid)
							}
							alloc.AssertNotCalled(t, "AllocID", mock.Anything)
						} else {
							require.Contains(t, status.GetReason(), "passed JSON software gate")
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

func TestRestoreJSONArraySoftwareGate(t *testing.T) {
	resetJSONVersionTargets(t)
	for _, indexType := range []string{indexparamcheck.IndexHybrid, indexparamcheck.IndexSTLSORT} {
		for _, cast := range []string{"ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR"} {
			for _, enabled := range []bool{false, true} {
				versions := []int32{common.CurrentScalarIndexEngineVersion}
				if enabled {
					versions = []int32{2, 3, common.CurrentScalarIndexEngineVersion}
				}
				for _, version := range versions {
					t.Run(fmt.Sprintf("%s/%s/v%d/enabled=%t", indexType, cast, version, enabled), func(t *testing.T) {
						setJSONArrayIndexGate(t, enabled)
						manager := &snapshotManager{indexEngineVersionManager: jsonArrayVersionManager(t, version)}
						err := manager.checkJSONPathIndexVersion(&model.Index{IndexParams: []*commonpb.KeyValuePair{
							{Key: common.IndexTypeKey, Value: indexType},
							{Key: common.JSONCastTypeKey, Value: cast},
							{Key: common.JSONPathKey, Value: "/a"},
						}})
						if !enabled {
							require.ErrorIs(t, err, merr.ErrServiceNotReady)
						} else if version < 3 {
							require.ErrorIs(t, err, merr.ErrParameterInvalid)
						} else {
							require.NoError(t, err)
						}
					})
				}
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
			setJSONArrayIndexGate(t, false)
			manager := &snapshotManager{indexEngineVersionManager: jsonArrayVersionManager(t, tc.version)}
			require.NoError(t, manager.checkJSONPathIndexVersion(&model.Index{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: tc.indexType},
				{Key: common.JSONCastTypeKey, Value: tc.cast},
				{Key: common.JSONPathKey, Value: "/a"},
			}}))
		})
	}
}

func TestRestoreOrdinaryArraySortSoftwareGate(t *testing.T) {
	resetJSONVersionTargets(t)
	for _, tc := range []struct {
		name     string
		field    *schemapb.FieldSchema
		enabled  bool
		version  int32
		expected error
	}{
		{"disabled", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, false, common.CurrentScalarIndexEngineVersion, merr.ErrServiceNotReady},
		{"legacy_format", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, true, 2, merr.ErrParameterInvalid},
		{"packed_v3", &schemapb.FieldSchema{Name: "values", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, true, 3, nil},
		{"existing_nested", &schemapb.FieldSchema{Name: "items[value]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, false, 4, nil},
		{"existing_scalar", &schemapb.FieldSchema{Name: "value", DataType: schemapb.DataType_Int64}, false, common.CurrentScalarIndexEngineVersion, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setJSONArrayIndexGate(t, tc.enabled)
			tc.field.FieldID = 100
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Once()
			sm := &snapshotManager{
				meta: newMetaWithCollections(&collectionInfo{
					ID: 1, DatabaseName: "default", Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{tc.field}},
				}),
				allocator: alloc, indexEngineVersionManager: jsonArrayVersionManager(t, tc.version),
			}
			broadcastStarted := false
			err := sm.RestoreIndexes(context.Background(), &snapshotstorage.SnapshotData{Indexes: []*indexpb.IndexInfo{{
				FieldID: 100, IndexName: "array_sort", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexSTLSORT}},
			}}}, 1, func(context.Context, int64, string) (broadcaster.BroadcastAPI, error) {
				broadcastStarted = true
				return nil, merr.WrapErrServiceInternalMsg("passed array SORT gate")
			}, "snapshot")
			if tc.expected != nil {
				require.ErrorIs(t, err, tc.expected)
				require.False(t, broadcastStarted)
			} else {
				require.True(t, broadcastStarted)
				require.ErrorContains(t, err, "passed array SORT gate")
			}
		})
	}
}
