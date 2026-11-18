package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestRoutingExclusionsPreserveExactIdentity(t *testing.T) {
	key := QueryViewKey{ShardID: ShardID{VChannel: "a", ReplicaID: 11}, QueryViewVersion: QueryViewVersion{DataVersion: DataVersion{StreamingVersion: 2, CompactVersion: 3}, QueryVersion: 4}}
	constraints := &viewpb.QueryRoutingConstraints{
		ExcludedQueryNodeIds: []int64{101},
		ExcludedQueryViews: []*viewpb.QueryViewIdentity{
			{ReplicaId: key.ShardID.ReplicaID, Version: key.QueryViewVersion.IntoProto()},
			{ReplicaId: 22}, // A missing version must not exclude an entire replica.
		},
	}
	exclusions := NewQueryRoutingExclusions("a", constraints)
	plan := &viewpb.QueryPlan{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()}
	require.True(t, exclusions.ExcludesPlan(plan))
	for _, change := range []func(*viewpb.QueryPlan){
		func(p *viewpb.QueryPlan) { p.ShardId.Vchannel = "b" },
		func(p *viewpb.QueryPlan) { p.ShardId.ReplicaId++ },
		func(p *viewpb.QueryPlan) { p.Version.DataVersion.StreamingVersion++ },
		func(p *viewpb.QueryPlan) { p.Version.DataVersion.CompactVersion++ },
		func(p *viewpb.QueryPlan) { p.Version.QueryVersion++ },
	} {
		other := proto.Clone(plan).(*viewpb.QueryPlan)
		change(other)
		require.False(t, exclusions.ExcludesPlan(other))
	}
	plan.ShardId.ReplicaId = 22
	plan.WorkNodes = []*viewpb.QueryPlanWorkNode{
		{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}},
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 102}}},
	}
	require.False(t, exclusions.ExcludesPlan(plan))
	plan.WorkNodes[1].GetQueryNode().NodeId = 101
	require.True(t, exclusions.ExcludesPlan(plan))
	// A request snapshot owns its lookups, independent of later caller mutation.
	constraints.ExcludedQueryNodeIds[0] = 102
	constraints.ExcludedQueryViews[0].Version.QueryVersion++
	require.True(t, exclusions.ExcludesNode(101))
	require.False(t, exclusions.ExcludesNode(102))
	require.True(t, exclusions.ExcludesView(key))
}

func TestEmptyRoutingConstraintsExcludeNothing(t *testing.T) {
	for _, constraints := range []*viewpb.QueryRoutingConstraints{nil, {}} {
		exclusions := NewQueryRoutingExclusions("a", constraints)
		require.Nil(t, exclusions)
		require.False(t, exclusions.ExcludesNode(1))
		require.False(t, exclusions.ExcludesView(QueryViewKey{}))
		require.False(t, exclusions.ExcludesPlan(nil))
	}
}
