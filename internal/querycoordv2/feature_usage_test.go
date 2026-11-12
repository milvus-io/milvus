// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryViewFeatureUsageEntries(t *testing.T) {
	s, up := newHandoffServer(t)
	store := s.qviewsRuntime.loadConfigStore
	cfg := store.GetConfig(1).Clone()
	cfg.RequiredFields = []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}, {FieldId: 102}, {FieldId: 103}}
	cfg.LoadFields = []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}, {FieldId: 102}}
	cfg.Replicas[0].ResourceGroup = "custom"
	require.NoError(t, store.Put(t.Context(), cfg))
	check := func(loaded, partial, custom int64, cc featureusage.CollectionContext) {
		got := indexFeatureEntries(s.FeatureUsageEntries(t.Context(), cc))
		require.EqualValues(t, loaded, got[featureusage.GroupLoaded+"/"+featureusage.LoadedCollections].Value)
		require.EqualValues(t, partial, got[featureusage.GroupLoaded+"/"+featureusage.LoadedFieldsSubset].Value)
		require.EqualValues(t, custom, got[featureusage.GroupLoaded+"/"+featureusage.LoadedCustomResourceGroups].Value)
	}
	cc := featureusage.CollectionContext{}
	check(0, 0, 0, cc) // Up views still use the old configuration.
	up("v0", 1)
	check(0, 0, 0, cc) // The complete vchannel set is required.
	up("v1", 1)
	check(1, 1, 1, cc)                                                          // RequiredFields includes dynamic and flattened struct fields.
	check(0, 0, 0, featureusage.CollectionContext{FieldCount: map[int64]int{}}) // Dropped in RootCoord.
	cfg = cfg.Clone()
	cfg.LoadFields = nil // Empty hints mean full preload.
	require.NoError(t, store.Put(t.Context(), cfg))
	up("v0", 1)
	up("v1", 1)
	check(1, 0, 1, cc)
	cfg = cfg.Clone()
	cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 12, ResourceGroup: "custom"})
	require.NoError(t, store.Put(t.Context(), cfg))
	up("v0", 1)
	up("v1", 1)
	check(0, 0, 0, cc) // A replica with no registry entries is not loaded.
}

func (suite *ServiceSuite) TestCollectQueryNodeFeatureUsage() {
	ctx := context.Background()
	server := suite.server
	req := &internalpb.GetFeatureUsageRequest{}
	suite.Require().GreaterOrEqual(len(suite.nodes), 3)

	dead := suite.nodes[0]
	notReady := suite.nodes[1]
	for _, node := range suite.nodes {
		switch node {
		case dead:
			suite.cluster.EXPECT().GetFeatureUsage(mock.Anything, node, req).Return(nil, errors.New("connection refused"))
		case notReady:
			suite.cluster.EXPECT().GetFeatureUsage(mock.Anything, node, req).Return(&internalpb.GetFeatureUsageResponse{
				Status: merr.Status(merr.ErrServiceNotReady),
			}, nil)
		default:
			suite.cluster.EXPECT().GetFeatureUsage(mock.Anything, node, req).Return(&internalpb.GetFeatureUsageResponse{
				Status:          merr.Success(),
				Role:            typeutil.QueryNodeRole,
				NodeId:          node,
				NodeStartTime:   100 + node,
				CountersEnabled: true,
				Entries: []*internalpb.FeatureEntry{
					{Group: featureusage.GroupRequest, Name: featureusage.FeatureTwoStageSearch.Name(), Value: 5, LastUsedAt: 200},
					featureusage.BoolConfigEntry("queryNode.enableDisk", true),
				},
			}, nil)
		}
	}

	nodes := server.CollectQueryNodeFeatureUsage(ctx, req)
	suite.Require().Len(nodes, len(suite.nodes), "an unreachable node is reported, never omitted")
	for i := 1; i < len(nodes); i++ {
		suite.Less(nodes[i-1].NodeId, nodes[i].NodeId, "sorted by node id")
	}
	byID := make(map[int64]*internalpb.FeatureUsageNode)
	for _, n := range nodes {
		suite.Equal(typeutil.QueryNodeRole, n.Role)
		byID[n.NodeId] = n
	}
	suite.False(byID[dead].Reachable)
	suite.Contains(byID[dead].Error, "connection refused")
	suite.Empty(byID[dead].Entries)
	suite.False(byID[notReady].Reachable)
	suite.NotEmpty(byID[notReady].Error)
	for _, node := range suite.nodes[2:] {
		n := byID[node]
		suite.True(n.Reachable)
		suite.Empty(n.Error)
		suite.EqualValues(100+node, n.NodeStartTime)
		suite.True(n.CountersEnabled)
		suite.Len(n.Entries, 2)
	}

	// A server without node manager or cluster reports nothing rather than panicking.
	suite.Nil((&Server{}).CollectQueryNodeFeatureUsage(ctx, req))
	suite.Nil((&Server{}).FeatureUsageEntries(ctx, featureusage.CollectionContext{}))
}

// indexFeatureEntries keys entries by group/name for entries without a bucket.
func indexFeatureEntries(entries []*internalpb.FeatureEntry) map[string]*internalpb.FeatureEntry {
	out := make(map[string]*internalpb.FeatureEntry, len(entries))
	for _, e := range entries {
		if e.Bucket == "" {
			out[e.Group+"/"+e.Name] = e
		}
	}
	return out
}
