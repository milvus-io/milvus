package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// This file holds querycoord's barriers on analyzer file resources: the
// dictionaries, stop-word lists and synonym files an analyzer needs on local
// disk before it can resolve a resource an analyzer parameter names.
//
// A query node becomes visible to querycoord as soon as its session registers,
// which is earlier than its file resource download finishes. Anything that puts
// data on it before then - a segment carrying a BM25 field, a channel whose
// delegator runs an analyzer over the growing data - races that download.
//
// Each barrier answers nil when no file resource is registered at all, which is
// every deployment that does not use them.

// checkFileResourceReadyForResourceGroups reports whether the query nodes of
// the named resource groups hold the current analyzer file resources.
//
// It is scoped to the resource groups the request names so that a lagging node
// elsewhere cannot block an unrelated load.
//
// A resource group the meta store does not know is not this check's error to
// report: the load path itself rejects it, with a message about the resource
// group rather than about analyzer files.
func (s *Server) checkFileResourceReadyForResourceGroups(ctx context.Context, rgNames []string) error {
	if s.fileResourceObserver == nil {
		return nil
	}
	// A load that names no resource group loads into the default one, so an
	// empty list is the default group by another spelling - it must be
	// checked exactly as the explicit __default_resource_group is, or the
	// same load passes or fails on how the client happened to phrase it.
	if len(rgNames) == 0 {
		rgNames = []string{meta.DefaultResourceGroupName}
	}
	nodes := make([]int64, 0, len(rgNames))
	for _, rgName := range rgNames {
		if rgName == "" {
			rgName = meta.DefaultResourceGroupName
		}
		rgNodes, err := s.meta.GetNodes(ctx, rgName)
		if err != nil {
			continue
		}
		nodes = append(nodes, rgNodes...)
	}
	if len(nodes) == 0 {
		return nil
	}
	return s.fileResourceObserver.CheckNodesSynced(nodes)
}

// nodeFileResourceGate returns the gate the task executors defer grow actions
// on, or nil when the observer is absent and the executors consult nothing.
//
// The load-path check above only sees the resource group membership at the
// moment the load request arrives. It does not cover a node that crashes and
// rejoins, is rolling-replaced, or joins the resource group afterwards: those
// paths put segments and channels back through the checkers, with no load
// request involved. Gating the action itself is what makes "a node serving a
// shard holds the files its analyzers need" hold on every path.
func (s *Server) nodeFileResourceGate() task.NodeFileResourceGate {
	if s.fileResourceObserver == nil {
		return nil
	}
	observer := s.fileResourceObserver
	return func(nodeID int64) error {
		return observer.CheckNodesSynced([]int64{nodeID})
	}
}

// notifyFileResourceOnNodeRediscovery asks the observer to sync file resources
// after querycoord rediscovers the nodes already in the session directory.
//
// Those nodes never went through the session-add path, which is the only thing
// that notifies the observer natively, so nothing else would trigger a sync for
// them. That is harmless when nothing gates on the result and a deadlock when
// something does: a node rediscovered on querycoord restart or on an etcd
// reconnect would sit behind the gate above until an unrelated event happened
// to notify the observer.
func (s *Server) notifyFileResourceOnNodeRediscovery() {
	if s.fileResourceObserver == nil {
		return
	}
	s.fileResourceObserver.Notify()
}
