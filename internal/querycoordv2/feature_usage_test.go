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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (suite *ServiceSuite) TestFeatureUsageEntries() {
	ctx := context.Background()
	server := suite.server

	// Nothing loaded: the group is still present, with zeros.
	got := indexFeatureEntries(server.FeatureUsageEntries(ctx, featureusage.CollectionContext{}))
	suite.Require().Contains(got, featureusage.GroupLoaded+"/"+featureusage.LoadedCollections)
	suite.EqualValues(0, got[featureusage.GroupLoaded+"/"+featureusage.LoadedCollections].Value)

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102}}}
	// Fully loaded, 2 replicas, default resource group only.
	suite.Require().NoError(suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 1000, ReplicaNumber: 2, Status: querypb.LoadStatus_Loaded, LoadFields: []int64{100, 101, 102}},
		Schema:             schema,
	}))
	suite.Require().NoError(suite.meta.Put(ctx,
		meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1000, ResourceGroup: meta.DefaultResourceGroupName}),
		meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 1000, ResourceGroup: meta.DefaultResourceGroupName}),
	))
	// Partial load, 1 replica in a custom resource group.
	suite.Require().NoError(suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 1001, ReplicaNumber: 1, Status: querypb.LoadStatus_Loaded, LoadFields: []int64{100}},
		Schema:             schema,
	}))
	suite.Require().NoError(suite.meta.Put(ctx,
		meta.NewReplica(&querypb.Replica{ID: 3, CollectionID: 1001, ResourceGroup: "rg_custom"}),
	))

	entries := server.FeatureUsageEntries(ctx, featureusage.CollectionContext{})
	got = indexFeatureEntries(entries)
	suite.EqualValues(2, got[featureusage.GroupLoaded+"/"+featureusage.LoadedCollections].Value)
	suite.EqualValues(1, got[featureusage.GroupLoaded+"/"+featureusage.LoadedFieldsSubset].Value)
	suite.EqualValues(1, got[featureusage.GroupLoaded+"/"+featureusage.LoadedCustomResourceGroups].Value)
	replicaBuckets := 0
	for _, e := range entries {
		if e.Group == featureusage.GroupDist && e.Name == featureusage.DistLoadedReplicaNumber {
			replicaBuckets++
			suite.NotEmpty(e.Bucket)
		}
		suite.NotContains(e.Name, "rg_custom", "resource group names never appear in the report")
		suite.NotContains(e.Bucket, "rg_custom")
	}
	suite.Equal(2, replicaBuckets, "replica numbers 1 and 2 land in two buckets")
}

// The loaded group resolves collections against the RootCoord snapshot when
// one is given: a recovered QueryCoord holds no schema, and a collection being
// dropped is still here but no longer there.
func (suite *ServiceSuite) TestFeatureUsageEntriesWithCollectionContext() {
	ctx := context.Background()
	server := suite.server
	loaded := func(id int64, fields []int64, status querypb.LoadStatus) {
		suite.Require().NoError(suite.meta.PutCollection(ctx, &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: id, ReplicaNumber: 1, Status: status, LoadFields: fields},
			// No Schema: this is what CollectionManager.Recover leaves behind.
		}))
	}
	loaded(2000, []int64{100}, querypb.LoadStatus_Loaded)           // partial load, recovered
	loaded(2001, []int64{100, 101, 102}, querypb.LoadStatus_Loaded) // full load, recovered
	loaded(2002, nil, querypb.LoadStatus_Loaded)                    // full load, no list
	loaded(2003, []int64{100}, querypb.LoadStatus_Loading)          // still loading
	loaded(2004, []int64{100}, querypb.LoadStatus_Loaded)           // dropped in RootCoord

	key := featureusage.GroupLoaded + "/"
	// Without a snapshot the recovered partial load is invisible.
	got := indexFeatureEntries(server.FeatureUsageEntries(ctx, featureusage.CollectionContext{}))
	suite.EqualValues(4, got[key+featureusage.LoadedCollections].Value, "loading excluded, dropped not knowable")
	suite.EqualValues(0, got[key+featureusage.LoadedFieldsSubset].Value)

	cc := featureusage.CollectionContext{FieldCount: map[int64]int{2000: 3, 2001: 3, 2002: 3, 2003: 3}}
	got = indexFeatureEntries(server.FeatureUsageEntries(ctx, cc))
	suite.EqualValues(3, got[key+featureusage.LoadedCollections].Value, "loading and dropped excluded")
	suite.EqualValues(1, got[key+featureusage.LoadedFieldsSubset].Value, "the recovered partial load counts")
}

func TestIsLoadFieldsSubset(t *testing.T) {
	assert.False(t, isLoadFieldsSubset(nil, 3), "no list is a full load")
	assert.False(t, isLoadFieldsSubset([]int64{1, 2, 3}, 3))
	assert.True(t, isLoadFieldsSubset([]int64{1}, 3))
	assert.False(t, isLoadFieldsSubset([]int64{1}, 0), "unknown schema never counts")
	assert.Equal(t, 0, loadableFieldCount(nil))
	assert.Equal(t, 3, loadableFieldCount(&schemapb.CollectionSchema{
		Fields:            []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}, {FieldID: 100}, {FieldID: 101}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{Fields: []*schemapb.FieldSchema{{FieldID: 102}}}},
	}), "system fields are not loadable; struct sub-fields are")
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
				Status:        merr.Success(),
				Role:          typeutil.QueryNodeRole,
				NodeId:        node,
				NodeStartTime: 100 + node,
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
