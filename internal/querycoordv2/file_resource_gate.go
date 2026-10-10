package querycoordv2

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// This file holds querycoord's barrier on analyzer file resources: the
// dictionaries, stop-word lists and synonym files an analyzer needs on local
// disk before it can resolve a resource an analyzer parameter names.
//
// A query node becomes visible to querycoord as soon as its session registers,
// which is earlier than its file resource download finishes. Anything that puts
// data on it before then - a segment carrying a BM25 field, a channel whose
// delegator runs an analyzer over the growing data - races that download.
//
// The barrier answers nil when no file resource is registered at all, which is
// every deployment that does not use them.

// nodeFileResourceGate returns the gate the task executors defer grow actions
// on, or nil when the observer is absent and the executors consult nothing.
//
// The gate sits on the action rather than on the load request. A load-time
// check would only see the resource group membership at the moment the request
// arrives: it would not cover a node that crashes and rejoins, is
// rolling-replaced, or joins the resource group afterwards, because those paths
// put segments and channels back through the checkers with no load request
// involved. Gating the action is what makes "a node serving a shard holds the
// files its analyzers need" hold on every path - and it defers the action
// instead of failing the user's load, so a node still downloading delays
// placement rather than turning the load into an error.
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
