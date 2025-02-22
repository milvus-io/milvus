package balancer

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// policies is a map of registered balancer policies.
var policies typeutil.ConcurrentMap[string, Policy]

// CurrentLayout is the full topology of streaming node and pChannel.
type CurrentLayout struct {
	IncomingChannels []string                          // IncomingChannels is the channels that are waiting for assignment (not assigned in AllNodesInfo).
	AllNodesInfo     map[int64]types.StreamingNodeInfo // AllNodesInfo is the full information of all available streaming nodes and related pchannels (contain the node not assign anything on it).
	AssignedChannels map[int64][]types.PChannelInfo    // AssignedChannels maps the node id to assigned channels.
	ChannelsToNodes  map[string]int64                  // ChannelsToNodes maps assigned channel name to node id.
}

// TotalChannels returns the total number of channels in the layout.
func (layout *CurrentLayout) TotalChannels() int {
	return len(layout.IncomingChannels) + len(layout.ChannelsToNodes)
}

// TotalNodes returns the total number of nodes in the layout.
func (layout *CurrentLayout) TotalNodes() int {
	return len(layout.AllNodesInfo)
}

// ExpectedLayout is the expected layout of streaming node and pChannel.
type ExpectedLayout struct {
	ChannelAssignment map[string]types.StreamingNodeInfo // ChannelAssignment is the assignment of channel to node.
}

// Policy is a interface to define the policy of rebalance.
type Policy interface {
	// Name is the name of the policy.
	Name() string

	// Balance is a function to balance the load of streaming node.
	// 1. all channel should be assigned.
	// 2. incoming layout should not be changed.
	// 3. return a expected layout.
	// 4. otherwise, error must be returned.
	// return a map of channel to a list of balance operation.
	// All balance operation in a list will be executed in order.
	// different channel's balance operation can be executed concurrently.
	Balance(currentLayout CurrentLayout) (expectedLayout ExpectedLayout, err error)
}

// RegisterPolicy registers balancer policy.
func RegisterPolicy(p Policy) {
	_, loaded := policies.GetOrInsert(p.Name(), p)
	if loaded {
		panic("policy already registered: " + p.Name())
	}
}

// mustGetPolicy returns the walimpls builder by name.
func mustGetPolicy(name string) Policy {
	b, ok := policies.Get(name)
	if !ok {
		panic("policy not found: " + name)
	}
	return b
}
