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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLoadConfigCompliance(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name                                                                     string
		target                                                                   map[string]int32
		actual                                                                   []string
		clusterRGs, clusterCount                                                 string
		user, force, released, invisible, unserviceable, ro, rosq, leak, unknown bool
		want                                                                     map[string]bool
		global                                                                   bool
		sq, legacySQ                                                             bool
	}{
		{name: "missing B does not block A", target: map[string]int32{"A": 1, "B": 1}, actual: []string{"A"}, want: map[string]bool{"A": true, "B": false}, global: true},
		{name: "excess B does not block A", target: map[string]int32{"A": 1}, actual: []string{"A", "B"}, want: map[string]bool{"A": true, "B": false}, global: true},
		{name: "single RG multiple replicas", clusterRGs: "A", clusterCount: "2", actual: []string{"A", "A"}, want: map[string]bool{"A": true}},
		{name: "user target takes precedence", target: map[string]int32{"A": 1}, actual: []string{"A"}, user: true, clusterRGs: "B", clusterCount: "1", want: map[string]bool{"A": true}},
		{name: "user missing replica checked", target: map[string]int32{"A": 1, "B": 1}, actual: []string{"A"}, user: true, want: map[string]bool{"A": true, "B": false}, global: true},
		{name: "force takes over target", target: map[string]int32{"A": 1}, actual: []string{"A"}, user: true, force: true, clusterRGs: "B", clusterCount: "1", want: map[string]bool{"A": false, "B": false}},
		{name: "incomplete override retains effective target", target: map[string]int32{"A": 1}, actual: []string{"A"}, force: true, clusterCount: "2", want: map[string]bool{"A": true}},
		{name: "invisible replica", target: map[string]int32{"A": 1, "B": 1}, actual: []string{"A", "B"}, invisible: true, want: map[string]bool{"A": true, "B": false}},
		{name: "unserviceable replica", target: map[string]int32{"A": 1, "B": 1}, actual: []string{"A", "B"}, unserviceable: true, want: map[string]bool{"A": true, "B": false}},
		{name: "RO resources block source", target: map[string]int32{"B": 1}, actual: []string{"B"}, ro: true, leak: true, want: map[string]bool{"A": false, "B": true}},
		{name: "RO SQ resources block source", target: map[string]int32{"B": 1}, actual: []string{"B"}, rosq: true, leak: true, want: map[string]bool{"A": false, "B": true}},
		{name: "streaming RO source uses session RG", target: map[string]int32{"B": 1}, actual: []string{"B"}, rosq: true, leak: true, unknown: true, sq: true, want: map[string]bool{"A": false, "B": true}},
		{name: "legacy streaming source uses default RG", target: map[string]int32{"B": 1}, actual: []string{"B"}, rosq: true, leak: true, unknown: true, sq: true, legacySQ: true, want: map[string]bool{meta.DefaultResourceGroupName: false, "B": true}},
		{name: "empty RO nodes do not block", target: map[string]int32{"B": 1}, actual: []string{"B"}, ro: true, want: map[string]bool{"B": true}},
		{name: "non replica resources block source", target: map[string]int32{"B": 1}, actual: []string{"B"}, leak: true, want: map[string]bool{"A": false, "B": true}},
		{name: "released collection residual resources", released: true, leak: true, want: map[string]bool{"A": false}},
		{name: "released collection residual replica", released: true, actual: []string{"B"}, want: map[string]bool{"B": false}, global: true},
		{name: "unknown resource owner blocks globally", released: true, leak: true, unknown: true, want: map[string]bool{}, global: true},
		{name: "no replica yet", target: map[string]int32{"A": 1}, want: map[string]bool{"A": false}, global: true},
		{name: "unknown legacy intent", actual: []string{"A"}, want: map[string]bool{"A": false}, global: true},
		{name: "empty cluster", released: true, want: map[string]bool{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &paramtable.Get().QueryCoordCfg
			values := map[string]string{cfg.ClusterLevelLoadReplicaNumber.Key: tc.clusterCount, cfg.ClusterLevelLoadResourceGroups.Key: tc.clusterRGs, cfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key: "false"}
			if tc.clusterCount == "" {
				values[cfg.ClusterLevelLoadReplicaNumber.Key] = "0"
			}
			if tc.force {
				values[cfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key] = "true"
			}
			for key, value := range values {
				require.NoError(t, paramtable.Get().Save(key, value))
				t.Cleanup(func() { paramtable.Get().Reset(key) })
			}
			nodes := session.NewNodeManager()
			if tc.sq {
				labels := map[string]string{sessionutil.LabelStreamingNodeEmbeddedQueryNode: "1"}
				if !tc.legacySQ {
					labels[sessionutil.LabelResourceGroup] = "A"
				}
				nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 99, Labels: labels}))
			}
			s := &Server{nodeMgr: nodes, meta: meta.NewMeta(func() (int64, error) { return 1, nil }, nil, nodes), dist: meta.NewDistributionManager(nodes)}
			defer mockey.Mock((*Server).State).Return(commonpb.StateCode_Healthy).Build().UnPatch()
			replicas := make([]*meta.Replica, 0, len(tc.actual))
			for i, rg := range tc.actual {
				pb := &querypb.Replica{ID: int64(i + 1), CollectionID: 100, ResourceGroup: rg, Nodes: []int64{int64(i + 10)}}
				if tc.ro {
					pb.RoNodes = []int64{99}
				}
				if tc.rosq {
					pb.RoSqNodes = []int64{99}
				}
				r := meta.NewReplica(pb, typeutil.NewUniqueSet())
				if tc.invisible && rg == "B" {
					mutable := r.CopyForWrite()
					mutable.SetQueryInvisible(true)
					r = mutable.IntoReplica()
				}
				replicas = append(replicas, r)
			}
			total := int32(0)
			for _, count := range tc.target {
				total += count
			}
			if tc.target == nil {
				total = int32(len(tc.actual))
			}
			collection := &meta.Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 100, ReplicaNumber: total, UserSpecifiedReplicaMode: tc.user, ResourceGroupReplicaNumbers: tc.target}}
			var collections []*meta.Collection
			if !tc.released {
				collections = []*meta.Collection{collection}
			}
			defer mockey.Mock((*meta.CollectionManager).GetAllCollections).Return(collections).Build().UnPatch()
			defer mockey.Mock((*meta.ReplicaManager).GetCollectionIDs).Return([]int64{100}).Build().UnPatch()
			defer mockey.Mock((*meta.ReplicaManager).GetByCollection).Return(replicas).Build().UnPatch()
			checked := 0
			defer mockey.Mock((*Server).checkReplicaServiceable).To(func(_ *Server, _ context.Context, r *meta.Replica) error {
				checked++
				if tc.unserviceable && r.GetResourceGroup() == "B" {
					return merr.WrapErrServiceUnavailableMsg("missing shard leader")
				}
				return nil
			}).Build().UnPatch()
			owner := "A"
			if tc.unknown {
				owner = ""
			}
			defer mockey.Mock((*meta.ResourceManager).GetResourceGroupByNodeID).Return(owner).Build().UnPatch()
			if tc.leak {
				s.dist.SegmentDistManager.Update(99, meta.SegmentFromInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 100}))
				s.dist.ChannelDistManager.Update(99, meta.DmChannelFromVChannel(&datapb.VchannelInfo{CollectionID: 100, ChannelName: "c1"}))
			}
			groups, err := s.GetLoadConfigCompliance(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.global, groups[""] != "")
			delete(groups, "")
			require.Len(t, groups, len(tc.want))
			for rg, ready := range tc.want {
				reason, ok := groups[rg]
				require.True(t, ok, rg)
				require.Equal(t, ready, reason == "", reason)
			}
			if !tc.released {
				require.Equal(t, len(replicas), checked)
			}
		})
	}
}

func TestLoadConfigComplianceUnhealthy(t *testing.T) {
	defer mockey.Mock((*Server).State).Return(commonpb.StateCode_Abnormal).Build().UnPatch()
	_, err := (&Server{}).GetLoadConfigCompliance(context.Background())
	require.Error(t, err)
}
