package qviews

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// QueryRoutingExclusions is an immutable lookup for one VChannel's planning
// attempt. Versions include DataVersion; query_version alone is not an identity.
type QueryRoutingExclusions struct {
	nodes map[int64]struct{}
	views map[QueryViewKey]struct{}
}

func NewQueryRoutingExclusions(vchannel string, constraints *viewpb.QueryRoutingConstraints) *QueryRoutingExclusions {
	if len(constraints.GetExcludedQueryNodeIds()) == 0 && len(constraints.GetExcludedQueryViews()) == 0 {
		return nil
	}
	e := &QueryRoutingExclusions{nodes: make(map[int64]struct{}), views: make(map[QueryViewKey]struct{})}
	for _, node := range constraints.GetExcludedQueryNodeIds() {
		e.nodes[node] = struct{}{}
	}
	for _, view := range constraints.GetExcludedQueryViews() {
		if view.GetVersion() == nil {
			continue
		}
		e.views[QueryViewKey{ShardID: ShardID{VChannel: vchannel, ReplicaID: view.ReplicaId}, QueryViewVersion: FromProtoQueryViewVersion(view.Version)}] = struct{}{}
	}
	return e
}

func (e *QueryRoutingExclusions) ExcludesNode(id int64) bool {
	if e == nil {
		return false
	}
	_, excluded := e.nodes[id]
	return excluded
}

func (e *QueryRoutingExclusions) ExcludesView(key QueryViewKey) bool {
	if e == nil {
		return false
	}
	_, excluded := e.views[key]
	return excluded
}

func (e *QueryRoutingExclusions) ExcludesPlan(plan *viewpb.QueryPlan) bool {
	if e == nil {
		return false
	}
	if e.ExcludesView(QueryViewKey{ShardID: FromProtoShardID(plan.ShardId), QueryViewVersion: FromProtoQueryViewVersion(plan.Version)}) {
		return true
	}
	for _, node := range plan.WorkNodes {
		if qn := node.GetQueryNode(); qn != nil && e.ExcludesNode(qn.NodeId) {
			return true
		}
	}
	return false
}
