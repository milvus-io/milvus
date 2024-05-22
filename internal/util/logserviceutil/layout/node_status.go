package layout

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

var ErrFreeze = errors.New("balancer: node is freezed")

type VersionedNodeStatusMap struct {
	Version int64
	status  *NodeStatus
}

// NodeStatus is the status of a log node.
type NodeStatus struct {
	ServerID          int64
	Address           string
	Channels          map[string]*logpb.PChannelInfo
	BalanceAttributes *logpb.LogNodeBalancerAttributes
	Error             error
}

// Clone clone a node status.
func (ns *NodeStatus) Clone() *NodeStatus {
	channels := make(map[string]*logpb.PChannelInfo, len(ns.Channels))
	for name, ch := range ns.Channels {
		channels[name] = proto.Clone(ch).(*logpb.PChannelInfo)
	}

	return &NodeStatus{
		ServerID:          ns.ServerID,
		Address:           ns.Address,
		Channels:          channels,
		BalanceAttributes: proto.Clone(ns.BalanceAttributes).(*logpb.LogNodeBalancerAttributes),
		Error:             ns.Error,
	}
}

// Update updates the node status with incoming status.
func (ns *NodeStatus) Update(incoming *NodeStatus) {
	for name, ch := range incoming.Channels {
		// Add new channel info.
		if _, ok := ns.Channels[name]; !ok {
			ns.Channels[name] = ch
			continue
		}
		// Skip old term update.
		if ch.Term >= ns.Channels[name].Term {
			ns.Channels[name] = ch
		}
	}
	ns.BalanceAttributes = incoming.BalanceAttributes
}
