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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestHandleReplicaLoadConfigCompliance(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name, count, rgs, primary, wal, badRG, invisibleRG, leakedRG string
		actual, next                                                 []string
		user, force, noCollections, leak, unknownSource              bool
		ready                                                        bool
		want                                                         map[string]bool
	}{
		{name: "no configuration", actual: []string{"A"}, ready: true, want: map[string]bool{"A": true}},
		{name: "single RG multiple replicas", count: "2", rgs: "A", actual: []string{"A", "A"}, ready: true, want: map[string]bool{"A": true}},
		{name: "repeated RG counts", count: "3", rgs: "A,B,B", actual: []string{"A", "B", "B"}, ready: true, want: map[string]bool{"A": true, "B": true}},
		{name: "missing B only blocks B", count: "2", rgs: "A,B", actual: []string{"A"}, want: map[string]bool{"A": true, "B": false}},
		{name: "extra B only blocks B", count: "1", rgs: "A", actual: []string{"A", "B"}, want: map[string]bool{"A": true, "B": false}},
		{name: "count mismatch still checks actual RG", count: "2", rgs: "A,B", actual: []string{"A"}, badRG: "A", want: map[string]bool{"A": false, "B": false}},
		{name: "count mismatch still checks next collection", count: "2", rgs: "A,B", actual: []string{"A"}, next: []string{"A", "B"}, badRG: "B", want: map[string]bool{"A": true, "B": false}},
		{name: "equal count wrong distribution", count: "2", rgs: "A,B", actual: []string{"A", "A"}, want: map[string]bool{"A": false, "B": false}},
		{name: "user config exempt", count: "1", rgs: "B", actual: []string{"A"}, user: true, ready: true, want: map[string]bool{"A": true}},
		{name: "user runtime still checked", count: "1", rgs: "B", actual: []string{"A"}, user: true, badRG: "A", want: map[string]bool{"A": false}},
		{name: "force override user config", count: "1", rgs: "B", actual: []string{"A"}, user: true, force: true, want: map[string]bool{"A": false, "B": false}},
		{name: "invisible replica", actual: []string{"A", "B"}, invisibleRG: "B", want: map[string]bool{"A": true, "B": false}},
		{name: "scale down residual", count: "1", rgs: "B", actual: []string{"B"}, leak: true, leakedRG: "A", want: map[string]bool{"A": false, "B": true}},
		{name: "unknown residual owner", actual: []string{"B"}, leak: true, want: map[string]bool{"B": true}},
		{name: "empty replica set", count: "1", rgs: "A", want: map[string]bool{"A": false}},
		{name: "empty loaded set", count: "1", rgs: "A", noCollections: true, ready: true, want: map[string]bool{}},
		{name: "incomplete count", count: "2", actual: []string{"A"}, want: map[string]bool{"A": true}},
		{name: "incomplete groups", rgs: "A", actual: []string{"A"}, want: map[string]bool{"A": true}},
		{name: "invalid config shape", count: "3", rgs: "A,B", actual: []string{"A"}, want: map[string]bool{"A": true}},
		{name: "WAL assigned target", primary: "B", wal: "assigned", noCollections: true, ready: true, want: map[string]bool{"B": true}},
		{name: "WAL source and target", primary: "B", wal: "old", noCollections: true, want: map[string]bool{"A": false, "B": false}},
		{name: "WAL assigning sources", primary: "B", wal: "assigning", noCollections: true, want: map[string]bool{"A": false, "B": false}},
		{name: "WAL unavailable", primary: "B", wal: "unavailable", noCollections: true, want: map[string]bool{"B": false}},
		{name: "WAL uninitialized", primary: "B", wal: "initial", noCollections: true, want: map[string]bool{"B": false}},
		{name: "WAL RO excluded", primary: "B", wal: "ro", noCollections: true, ready: true, want: map[string]bool{"B": true}},
		{name: "WAL unknown history", primary: "B", wal: "assigning", unknownSource: true, noCollections: true, want: map[string]bool{"B": false}},
		{name: "WAL failure still checks collections", primary: "B", wal: "old", actual: []string{"C"}, badRG: "C", want: map[string]bool{"A": false, "B": false, "C": false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count := tc.count
			if count == "" {
				count = "0"
			}
			force := "false"
			if tc.force {
				force = "true"
			}
			for key, value := range map[string]string{
				Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:                count,
				Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key:               tc.rgs,
				Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key: force,
				Params.StreamingCfg.PrimaryResourceGroup.Key:                          tc.primary,
			} {
				require.NoError(t, paramtable.Get().Save(key, value))
				t.Cleanup(func() { paramtable.Get().Reset(key) })
			}
			mode := types.AccessModeRW
			if tc.wal == "ro" {
				mode = types.AccessModeRO
			}
			ch := channel.NewPChannelMeta("p0", mode).CopyForWrite()
			if tc.wal != "initial" {
				ch.TryAssignToServerID(mode, types.StreamingNodeInfo{ServerID: 1})
				ch.AssignToServerDone()
				if tc.wal != "old" && tc.wal != "ro" {
					ch.TryAssignToServerID(mode, types.StreamingNodeInfo{ServerID: 2})
				}
				if tc.wal == "assigned" || tc.wal == "unavailable" {
					ch.AssignToServerDone()
				}
				if tc.wal == "unavailable" {
					ch.MarkAsUnavailable(ch.CurrentTerm())
				}
			}
			snapshot := &balancer.WatchChannelAssignmentsCallbackParam{PChannelView: &channel.PChannelView{Channels: map[types.ChannelID]*channel.PChannelMeta{ch.ChannelID(): ch.PChannelMeta}}}
			nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "A"}, 2: {ResourceGroup: "B"}}
			if tc.unknownSource {
				delete(nodes, 1)
			}
			defer mockey.Mock(balance.GetWithContext).Return(&mock_balancer.MockBalancer{}, nil).Build().UnPatch()
			defer mockey.Mock((*mock_balancer.MockBalancer).GetLatestChannelAssignment).Return(snapshot, nil).Build().UnPatch()
			defer mockey.Mock((*mock_balancer.MockBalancer).GetAllStreamingNodes).Return(nodes, nil).Build().UnPatch()
			ids := []int64{100}
			if tc.noCollections {
				ids = nil
			}
			if tc.next != nil {
				ids = append(ids, 200)
			}
			defer mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{Status: merr.Success(), CollectionIDs: ids}, nil).Build().UnPatch()
			replicas := make(map[int64][]*meta.Replica)
			for id, rgs := range map[int64][]string{100: tc.actual, 200: tc.next} {
				for i, rg := range rgs {
					r := meta.NewReplica(&querypb.Replica{ID: id + int64(i), CollectionID: id, ResourceGroup: rg})
					if rg == tc.invisibleRG {
						m := r.CopyForWrite()
						m.SetQueryInvisible(true)
						r = m.IntoReplica()
					}
					replicas[id] = append(replicas[id], r)
				}
			}
			defer mockey.Mock((*querycoordv2.Server).GetInternalReplicasByCollection).To(func(_ *querycoordv2.Server, _ context.Context, id int64) []*meta.Replica { return replicas[id] }).Build().UnPatch()
			defer mockey.Mock((*querycoordv2.Server).IsCollectionUserSpecifiedReplicaMode).Return(tc.user).Build().UnPatch()
			checked := 0
			defer mockey.Mock((*querycoordv2.Server).CheckReplicasServiceable).To(func(_ *querycoordv2.Server, _ context.Context, id int64) map[int64]error {
				checked++
				errs := make(map[int64]error)
				for _, r := range replicas[id] {
					if r.GetResourceGroup() == tc.badRG {
						errs[r.GetID()] = merr.WrapErrServiceUnavailableMsg("missing shard leader")
					}
				}
				return errs
			}).Build().UnPatch()
			leaks := map[string]int{}
			if tc.leak {
				leaks[tc.leakedRG] = 1
			}
			defer mockey.Mock((*querycoordv2.Server).GetLeakedResourcesByCollectionPerRG).Return(leaks).Build().UnPatch()
			coord := &mixCoordImpl{queryCoordServer: &querycoordv2.Server{}}
			var previousReason string
			for i, output := range []string{"", "summary", "per_resource_group"} {
				w := httptest.NewRecorder()
				coord.HandleReplicaLoadConfigCompliance(w, httptest.NewRequest(http.MethodGet, "/?output="+output, nil))
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				var resp LoadConfigComplianceResponse
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
				require.Equal(t, tc.ready, resp.State == LoadConfigComplianceStateReady, resp.Reason)
				if i > 0 {
					require.Equal(t, previousReason, resp.Reason)
				}
				previousReason = resp.Reason
				if output != "per_resource_group" {
					require.Nil(t, resp.ResourceGroups)
					continue
				}
				require.NotNil(t, resp.ResourceGroups)
				require.Len(t, *resp.ResourceGroups, len(tc.want))
				last := ""
				for _, g := range *resp.ResourceGroups {
					require.Greater(t, g.ResourceGroup, last)
					last = g.ResourceGroup
					want, ok := tc.want[g.ResourceGroup]
					require.True(t, ok)
					require.Equal(t, want, g.State == LoadConfigComplianceStateReady, g.Reason)
				}
			}
			require.Equal(t, 3*len(ids), checked, "every collection must be checked in every output mode")
		})
	}
}

func TestComplianceReadAndRequestErrors(t *testing.T) {
	paramtable.Init()
	for _, stage := range []string{"method", "output", "balancer", "assignment", "view", "nodes", "collections"} {
		t.Run(stage, func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(Params.StreamingCfg.PrimaryResourceGroup.Key, "B"))
			t.Cleanup(func() { paramtable.Get().Reset(Params.StreamingCfg.PrimaryResourceGroup.Key) })
			failure := merr.WrapErrServiceUnavailableMsg("metadata unavailable")
			var balErr, assignmentErr, nodesErr, showErr error
			snapshot := &balancer.WatchChannelAssignmentsCallbackParam{PChannelView: &channel.PChannelView{}}
			switch stage {
			case "balancer":
				balErr = failure
			case "assignment":
				assignmentErr = failure
			case "view":
				snapshot.PChannelView = nil
			case "nodes":
				nodesErr = failure
			case "collections":
				showErr = failure
			}
			defer mockey.Mock(balance.GetWithContext).Return(&mock_balancer.MockBalancer{}, balErr).Build().UnPatch()
			defer mockey.Mock((*mock_balancer.MockBalancer).GetLatestChannelAssignment).Return(snapshot, assignmentErr).Build().UnPatch()
			defer mockey.Mock((*mock_balancer.MockBalancer).GetAllStreamingNodes).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nodesErr).Build().UnPatch()
			defer mockey.Mock((*mixCoordImpl).ShowLoadCollections).Return(&querypb.ShowCollectionsResponse{Status: merr.Success()}, showErr).Build().UnPatch()
			method, url, code := http.MethodGet, "/", http.StatusInternalServerError
			if stage == "method" {
				method = http.MethodPost
				code = http.StatusMethodNotAllowed
			}
			if stage == "output" {
				url = "/?output=typo"
				code = http.StatusBadRequest
			}
			w := httptest.NewRecorder()
			(&mixCoordImpl{}).HandleReplicaLoadConfigCompliance(w, httptest.NewRequest(method, url, nil))
			require.Equal(t, code, w.Code, w.Body.String())
		})
	}
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
