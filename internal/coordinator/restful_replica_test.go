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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// registerTestBalancer installs a mock balancer that returns the given error from
// ConfirmPrimaryResourceGroupReady. Caller must invoke the returned cleanup.
func registerTestBalancer(t *testing.T, primaryRGErr error) func() {
	balance.ResetBalancer()
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().ConfirmPrimaryResourceGroupReady(mock.Anything).Return(primaryRGErr).Maybe()
	balance.Register(b)
	return balance.ResetBalancer
}

func TestHandleReplicaLoadConfigCompliance(t *testing.T) {
	paramtable.Init()

	t.Run("wrong HTTP method should fail", func(t *testing.T) {
		coord := &mixCoordImpl{}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Contains(t, w.Body.String(), "Method not allowed")
	})

	t.Run("no cluster config returns Ready", func(t *testing.T) {
		// Set cluster config with no constraints
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

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
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "false")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		defer registerTestBalancer(t, nil)()

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

		// Mock CheckAllReplicasServiceable to allow flow to reach later checks
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

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

	t.Run("user-specified replica mode skips only cluster-level replica compliance", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "false")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		defer registerTestBalancer(t, nil)()

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

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).IsCollectionUserSpecifiedReplicaMode).Return(true).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker3.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

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

	t.Run("user-specified replica mode still checks query visibility", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "false")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		defer registerTestBalancer(t, nil)()

		replica := meta.NewReplica(&querypb.Replica{
			ID:            1,
			CollectionID:  100,
			ResourceGroup: "rg1",
		}, typeutil.NewUniqueSet())
		mutableReplica := replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		replicas := []*meta.Replica{mutableReplica.IntoReplica()}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).IsCollectionUserSpecifiedReplicaMode).Return(true).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker3.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not query visible")
	})

	t.Run("user-specified replica mode still checks leaked resources", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "false")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		defer registerTestBalancer(t, nil)()

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

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).IsCollectionUserSpecifiedReplicaMode).Return(true).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker3.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(3, 0).Build()
		defer mockerLeak.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not fully released")
		assert.Contains(t, resp.Reason, "leaked segments=3")
	})

	t.Run("force override checks user-specified replica mode collection", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		defer registerTestBalancer(t, nil)()

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

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).IsCollectionUserSpecifiedReplicaMode).Return(true).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker3.UnPatch()

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
		defer registerTestBalancer(t, nil)()

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

		// Mock CheckAllReplicasServiceable to allow flow to reach later checks
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

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

	t.Run("primary resource group not ready returns NotReady", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, fmt.Errorf("pchannel p0 still on rg=rg_old, expected primary rg=rg_new (WAL migration in progress)"))()

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()
		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "WAL placement")
		assert.Contains(t, resp.Reason, "WAL migration in progress")
	})

	t.Run("delegator not serviceable returns NotReady", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Replica count matches (1 replica) — needed so the flow reaches the serviceable check
		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
		}
		mockerRep := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mockerRep.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).
			Return(fmt.Errorf("replica 1 (rg=rg1) channel c1 not serviceable: still catching up")).Build()
		defer mocker2.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()
		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not serviceable")
		assert.Contains(t, resp.Reason, "catching up")
	})

	t.Run("delegator not serviceable reason takes precedence over query-invisible", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replica := meta.NewReplica(&querypb.Replica{
			ID:            1,
			CollectionID:  100,
			ResourceGroup: "rg1",
		}, typeutil.NewUniqueSet())
		mutableReplica := replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		replicas := []*meta.Replica{mutableReplica.IntoReplica()}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).
			Return(fmt.Errorf("replica 1 (rg=rg1) channel c1 not serviceable: delegator reported not serviceable")).Build()
		defer mocker3.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "delegator reported not serviceable")
		assert.NotContains(t, resp.Reason, "not query visible")
	})

	t.Run("query-invisible replica returns NotReady", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replica := meta.NewReplica(&querypb.Replica{
			ID:            1,
			CollectionID:  100,
			ResourceGroup: "rg1",
		}, typeutil.NewUniqueSet())
		mutableReplica := replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		replicas := []*meta.Replica{mutableReplica.IntoReplica()}

		coord := &mixCoordImpl{
			queryCoordServer: &querycoordv2.Server{},
		}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mocker3.UnPatch()

		mockerLeak := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mockerLeak.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not query visible")
	})

	t.Run("correct setup returns Ready", func(t *testing.T) {
		// Set cluster config requiring 2 replicas with specific resource groups
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

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

		// Mock CheckAllReplicasServiceable to skip the live dist check
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		// Mock GetLeakedResourcesByCollection to report no leaks
		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

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

	t.Run("leaked segments returns NotReady", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		// Simulate 5 segments still held by nodes no longer in any replica
		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(5, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()
		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not fully released")
		assert.Contains(t, resp.Reason, "leaked segments=5")
		assert.Contains(t, resp.Reason, "channels=0")
	})

	t.Run("leaked channels returns NotReady", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 2).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()
		coord.HandleReplicaLoadConfigCompliance(w, req)

		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "channels=2")
	})

	t.Run("internal error returns HTTP 500", func(t *testing.T) {
		// Set cluster config
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

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
		defer registerTestBalancer(t, nil)()

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

		// All replicas serviceable
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		// No leaked resources for either collection
		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
	})

	t.Run("per-resource-group mode reports each RG readiness", func(t *testing.T) {
		// Set cluster config requiring 2 replicas spread over rg1 and rg2
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		// Collection 100: replica in rg1 is query-invisible, replica in rg2 is fine
		rg1Replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet())
		mutableReplica := rg1Replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		rg1Replica = mutableReplica.IntoReplica()
		replicasMap := map[int64][]*meta.Replica{
			100: {
				rg1Replica,
				meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rg2"}, typeutil.NewUniqueSet()),
			},
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) []*meta.Replica {
			return replicasMap[collectionID]
		}).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).Return(map[int64]error{}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Len(t, *resp.ResourceGroups, 2)

		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range *resp.ResourceGroups {
			byRG[rg.ResourceGroup] = rg
		}
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rg1"].State)
		assert.Contains(t, byRG["rg1"].Reason, "not query visible")
		assert.Equal(t, LoadConfigComplianceStateReady, byRG["rg2"].State)
		assert.Empty(t, byRG["rg2"].Reason)
	})

	t.Run("per-resource-group mode continues past first failure", func(t *testing.T) {
		// No cluster-level RG constraint so the RG-distribution check does not interfere;
		// only the replica count and serviceability checks are exercised.
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicasMap := map[int64][]*meta.Replica{
			100: {
				// Two replicas on rg1 -> replica count mismatch (expected 1).
				meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
				meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
			},
			200: {
				meta.NewReplica(&querypb.Replica{ID: 3, CollectionID: 200, ResourceGroup: "rg2"}, typeutil.NewUniqueSet()),
			},
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100, 200},
			InMemoryPercentages: []int64{100, 100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) []*meta.Replica {
			return replicasMap[collectionID]
		}).Build()
		defer mocker2.UnPatch()

		// Collection 100 fails the replica count check and must be skipped entirely (no downstream
		// serviceability/leak checks); collection 200 fails serviceability. Recording the calls proves
		// the skip: 100 never reaches CheckReplicasServiceable.
		var svcChecked []int64
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) map[int64]error {
			svcChecked = append(svcChecked, collectionID)
			if collectionID == 200 {
				return map[int64]error{3: fmt.Errorf("replica 3 (rg=rg2) channel c1 not serviceable")}
			}
			return map[int64]error{}
		}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) map[string]int {
			if collectionID == 100 {
				t.Errorf("leak check ran for collection 100 despite replica count mismatch")
			}
			return map[string]int{}
		}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, []int64{200}, svcChecked)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		// Both collections were still checked (no fast-fail): rg1 failed the replica count
		// check and rg2 failed serviceability, so both are reported as not ready.
		assert.Len(t, *resp.ResourceGroups, 2)
		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range *resp.ResourceGroups {
			byRG[rg.ResourceGroup] = rg
		}
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rg1"].State)
		assert.Contains(t, byRG["rg1"].Reason, "replica count mismatch")
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rg2"].State)
		assert.Contains(t, byRG["rg2"].Reason, "not serviceable")
	})

	t.Run("per-resource-group mode keeps only first reason per RG", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		// Single replica in rg1, count OK. Both serviceability and query-visibility fail for it;
		// only the first reason (serviceability, checked before query-visibility) must be reported.
		rg1Replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet())
		mutableReplica := rg1Replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		rg1Replica = mutableReplica.IntoReplica()
		replicas := []*meta.Replica{rg1Replica}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).
			Return(map[int64]error{1: fmt.Errorf("replica 1 (rg=rg1) channel c1 not serviceable")}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Len(t, *resp.ResourceGroups, 1)
		assert.Equal(t, LoadConfigComplianceStateNotReady, (*resp.ResourceGroups)[0].State)
		assert.Contains(t, (*resp.ResourceGroups)[0].Reason, "not serviceable")
		assert.NotContains(t, (*resp.ResourceGroups)[0].Reason, "not query visible")
	})

	t.Run("per-resource-group mode all compliant returns Ready per RG", func(t *testing.T) {
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rg2"}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).Return(map[int64]error{}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
		assert.Len(t, *resp.ResourceGroups, 2)
		for _, rg := range *resp.ResourceGroups {
			assert.Equal(t, LoadConfigComplianceStateReady, rg.State)
			assert.Empty(t, rg.Reason)
		}
	})

	t.Run("per-resource-group mode global reason flips state to NotReady", func(t *testing.T) {
		// No cluster-level RG constraint and a collection with no replicas: the "no replica found"
		// violation cannot be attributed to any resource group and must flip the overall state to
		// NotReady (a caller using this endpoint as a gate must never see Ready here).
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		// Collection 100 has no replicas at all.
		mockerRep := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return([]*meta.Replica{}).Build()
		defer mockerRep.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).Return(map[int64]error{}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "no replica found")
	})

	t.Run("per-resource-group mode attributes distribution violation to offending RGs only", func(t *testing.T) {
		// Cluster config expects one replica on rg1 and one on rg2; the collection instead hosts
		// replicas on rg1 and rgX. Only rgX (extra) must be reported NotReady for the distribution
		// violation — rg1 (balanced) stays Ready and rg2 (missing) is NotReady.
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rgX"}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mockerRep := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mockerRep.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).Return(map[int64]error{}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range *resp.ResourceGroups {
			byRG[rg.ResourceGroup] = rg
		}
		// rg1 is balanced: one replica present, one expected.
		assert.Equal(t, LoadConfigComplianceStateReady, byRG["rg1"].State)
		// rg2 is missing a replica.
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rg2"].State)
		// rgX holds an unexpected replica.
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rgX"].State)
	})

	t.Run("per-resource-group mode attributes serviceability failure to the failing replica's RG only", func(t *testing.T) {
		// Collection hosts one replica on rg1 and one on rg2; only the rg2 replica is
		// unserviceable. rg1 must stay Ready — a healthy RG is not blocked by another RG's problem.
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rg2"}, typeutil.NewUniqueSet()),
		}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mockerRep := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mockerRep.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).
			Return(map[int64]error{2: fmt.Errorf("replica 2 (rg=rg2) channel c1 not serviceable")}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range *resp.ResourceGroups {
			byRG[rg.ResourceGroup] = rg
		}
		assert.Equal(t, LoadConfigComplianceStateReady, byRG["rg1"].State)
		assert.Empty(t, byRG["rg1"].Reason)
		assert.Equal(t, LoadConfigComplianceStateNotReady, byRG["rg2"].State)
		assert.Contains(t, byRG["rg2"].Reason, "not serviceable")
	})

	t.Run("per-resource-group mode invalid output value returns 400", func(t *testing.T) {
		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=bogus", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "invalid output")
	})

	t.Run("per-resource-group mode empty replica RG produces no blank entry", func(t *testing.T) {
		// A replica whose ResourceGroup is empty cannot be attributed to a named group: the violation
		// must surface as the global reason and flip the overall state, without emitting an entry like
		// {"resourceGroup":"","state":"NotReady"} that callers cannot act on.
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer registerTestBalancer(t, nil)()

		rg1Replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: ""}, typeutil.NewUniqueSet())
		mutableReplica := rg1Replica.CopyForWrite()
		mutableReplica.SetQueryInvisible(true)
		rg1Replica = mutableReplica.IntoReplica()
		replicas := []*meta.Replica{rg1Replica}

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		mocker1 := mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{
			Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			CollectionIDs:       []int64{100},
			InMemoryPercentages: []int64{100},
		}, nil).Build()
		defer mocker1.UnPatch()

		mocker2 := mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).Return(replicas).Build()
		defer mocker2.UnPatch()

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).Return(map[int64]error{}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(map[string]int{}).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "not query visible")
		// No blank resource-group entry: the violation went to the global reason instead.
		assert.Empty(t, resp.ResourceGroups)
	})

	t.Run("per-resource-group mode WAL not ready marks primary RG NotReady", func(t *testing.T) {
		// WAL placement is the primary resource group's obligation: when it is not ready, only that
		// group is reported NotReady with the WAL reason; other groups (here rg2, configured but
		// hosting no violation) stay Ready and are still listed since they are expected groups.
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
		paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
		paramtable.Get().Save(Params.StreamingCfg.PrimaryResourceGroup.Key, "rg1")
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
		defer paramtable.Get().Reset(Params.StreamingCfg.PrimaryResourceGroup.Key)
		defer registerTestBalancer(t, fmt.Errorf("streaming WAL not migrated yet"))()

		coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?output=per_resource_group", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Contains(t, resp.Reason, "WAL placement")
		assert.Len(t, *resp.ResourceGroups, 2)
		for _, rg := range *resp.ResourceGroups {
			if rg.ResourceGroup == "rg1" {
				assert.Equal(t, LoadConfigComplianceStateNotReady, rg.State)
				assert.Contains(t, rg.Reason, "WAL placement")
			} else {
				assert.Equal(t, "rg2", rg.ResourceGroup)
				assert.Equal(t, LoadConfigComplianceStateReady, rg.State)
				assert.Empty(t, rg.Reason)
			}
		}
	})
}

func TestValidateRGDistribution(t *testing.T) {
	coord := &mixCoordImpl{}

	t.Run("exact match returns empty reason", func(t *testing.T) {
		reason, offending := coord.validateRGDistribution(
			[]string{"rg1", "rg2"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)
		assert.Empty(t, offending)
	})

	t.Run("order independent match returns empty reason", func(t *testing.T) {
		reason, offending := coord.validateRGDistribution(
			[]string{"rg2", "rg1"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)
		assert.Empty(t, offending)
	})

	t.Run("missing expected RG returns reason", func(t *testing.T) {
		reason, offending := coord.validateRGDistribution(
			[]string{"rg1"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "resource group mismatch")
		assert.Contains(t, reason, "collection 100")
		assert.ElementsMatch(t, []string{"rg2"}, offending)
	})

	t.Run("extra actual RG returns reason", func(t *testing.T) {
		reason, offending := coord.validateRGDistribution(
			[]string{"rg1", "rg2", "rg3"},
			[]string{"rg1", "rg2"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "resource group mismatch")
		assert.ElementsMatch(t, []string{"rg3"}, offending)
	})

	t.Run("duplicate handling", func(t *testing.T) {
		// Both have duplicates, matching distribution
		reason, offending := coord.validateRGDistribution(
			[]string{"rg1", "rg1"},
			[]string{"rg1", "rg1"},
			"resource group",
			100,
		)
		assert.Empty(t, reason)
		assert.Empty(t, offending)

		// Actual has different duplicate count
		reason, offending = coord.validateRGDistribution(
			[]string{"rg1", "rg1", "rg1"},
			[]string{"rg1", "rg1"},
			"resource group",
			100,
		)
		assert.Contains(t, reason, "mismatch")
		assert.ElementsMatch(t, []string{"rg1"}, offending)
	})
}
