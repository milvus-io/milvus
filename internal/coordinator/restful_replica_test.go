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

package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestHandleReplicaLoadConfigCompliance(t *testing.T) {
	paramtable.Init()

	t.Run("no cluster config returns Ready", func(t *testing.T) {
		// Set cluster config with no constraints
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections to return empty (no collections loaded)
		mocker := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{},
			InMemoryPercentages: []int64{},
		}, nil).Build()
		defer mocker.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
		assert.Empty(t, resp.Reason)
	})

	t.Run("replica count mismatch returns NotReady", func(t *testing.T) {
		// Set cluster config requiring 2 replicas
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{
				ID:            1,
				CollectionID:  100,
				ResourceGroup: "rg1",
			}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections
		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Mock GetInternalReplicasByCollection
		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "replica count mismatch")
		assert.Contains(t, resp.Reason, "expected 2")
		assert.Contains(t, resp.Reason, "actual 1")
	})

	t.Run("resource group mismatch returns NotReady", func(t *testing.T) {
		// Set cluster config requiring specific resource groups
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{
				ID:            1,
				CollectionID:  100,
				ResourceGroup: "rg1",
			}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{
				ID:            2,
				CollectionID:  100,
				ResourceGroup: "rg1", // Wrong: should be rg2
			}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections
		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Mock GetInternalReplicasByCollection
		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "resource group mismatch")
	})

	t.Run("collection loading returns NotReady", func(t *testing.T) {
		// Set cluster config
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections with collection still loading (50%)
		mocker := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{50}, // Only 50% loaded
		}, nil).Build()
		defer mocker.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not fully loaded")
		assert.Contains(t, resp.Reason, "50%")
	})

	t.Run("correct setup returns Ready", func(t *testing.T) {
		// Set cluster config requiring 2 replicas with specific resource groups
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{
				ID:            1,
				CollectionID:  100,
				ResourceGroup: "rg1",
			}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{
				ID:            2,
				CollectionID:  100,
				ResourceGroup: "rg2",
			}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections
		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Mock GetInternalReplicasByCollection
		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
		assert.Empty(t, resp.Reason)
	})

	t.Run("internal error returns HTTP 500", func(t *testing.T) {
		// Set cluster config
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections to return error status
		mocker := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "internal error",
			},
		}, nil).Build()
		defer mocker.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "failed to get collections")
	})

	t.Run("multiple collections all compliant returns Ready", func(t *testing.T) {
		// Set cluster config
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

		replicasMap := map[int64][]*meta.Replica{
			100: {
				meta.NewReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  100,
					ResourceGroup: "rg1",
				}, typeutil.NewUniqueSet()),
			},
			200: {
				meta.NewReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  200,
					ResourceGroup: "rg1",
				}, typeutil.NewUniqueSet()),
			},
		}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		// Mock ShowLoadCollections with multiple collections
		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100, 200},
			InMemoryPercentages: []int64{100, 100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Mock GetInternalReplicasByCollection to return appropriate replicas
		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) []*meta.Replica {
			return replicasMap[collectionID]
		}).Build()
		defer mocker2.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
	})
}

func TestValidateRGDistribution(t *testing.T) {
	coord := &mixCoordImpl{}

	t.Run("exact match returns empty reason", func(t *testing.T) {
		reason := coord.validateRGDistribution(
			[]string{"rg1", "rg2"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)
	})

	t.Run("order independent match returns empty reason", func(t *testing.T) {
		reason := coord.validateRGDistribution(
			[]string{"rg2", "rg1"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)
	})

	t.Run("missing expected RG returns reason", func(t *testing.T) {
		reason := coord.validateRGDistribution(
			[]string{"rg1"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "resource group mismatch")
		assert.Contains(t, reason, "collection 100")
	})

	t.Run("extra actual RG returns reason", func(t *testing.T) {
		reason := coord.validateRGDistribution(
			[]string{"rg1", "rg2", "rg3"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "resource group mismatch")
	})

	t.Run("duplicate handling", func(t *testing.T) {
		// Both have duplicates, matching distribution
		reason := coord.validateRGDistribution(
			[]string{"rg1", "rg1"},
			[]string{"rg1", "rg1"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)

		// Actual has different duplicate count
		reason = coord.validateRGDistribution(
			[]string{"rg1", "rg1", "rg1"},
			[]string{"rg1", "rg1"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "mismatch")
	})
}
