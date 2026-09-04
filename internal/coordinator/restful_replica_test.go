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

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?per_resource_group=true", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Len(t, resp.ResourceGroups, 2)

		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range resp.ResourceGroups {
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

		// Collection 100 fails the replica count check; collection 200 fails serviceability.
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).To(func(_ *querycoordv2.Server, ctx context.Context, collectionID int64) error {
			if collectionID == 200 {
				return fmt.Errorf("replica 3 (rg=rg2) channel c1 not serviceable")
			}
			return nil
		}).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?per_resource_group=true", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		// Both collections were still checked (no fast-fail): rg1 failed the replica count
		// check and rg2 failed serviceability, so both are reported as not ready.
		assert.Len(t, resp.ResourceGroups, 2)
		byRG := map[string]ResourceGroupComplianceState{}
		for _, rg := range resp.ResourceGroups {
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

		replicas := []*meta.Replica{
			meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
			meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 100, ResourceGroup: "rg1"}, typeutil.NewUniqueSet()),
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

		// Both the replica count check (expected 1, actual 2) and the serviceability check fail
		// for rg1; only the first reason must be reported.
		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).
			Return(fmt.Errorf("replica 1 (rg=rg1) channel c1 not serviceable")).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?per_resource_group=true", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.State)
		assert.Len(t, resp.ResourceGroups, 1)
		assert.Equal(t, LoadConfigComplianceStateNotReady, resp.ResourceGroups[0].State)
		assert.Contains(t, resp.ResourceGroups[0].Reason, "replica count mismatch")
		assert.NotContains(t, resp.ResourceGroups[0].Reason, "not serviceable")
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

		mockerSvc := mockey.Mock((*querycoordv2.Server).CheckAllReplicasServiceable).Return(nil).Build()
		defer mockerSvc.UnPatch()

		mocker3 := mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollection).Return(0, 0).Build()
		defer mocker3.UnPatch()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/replicas/compliance?per_resource_group=true", nil)
		w := httptest.NewRecorder()

		coord.HandleReplicaLoadConfigCompliance(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp LoadConfigComplianceResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, LoadConfigComplianceStateReady, resp.State)
		assert.Len(t, resp.ResourceGroups, 2)
		for _, rg := range resp.ResourceGroups {
			assert.Equal(t, LoadConfigComplianceStateReady, rg.State)
			assert.Empty(t, rg.Reason)
		}
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
