package discoverer

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewChannelAssignmentDiscoverer returns a new Discoverer for the channel assignment registration.
func NewChannelAssignmentDiscoverer(logCoordManager types.AssignmentDiscoverWatcher) Discoverer {
	return &channelAssignmentDiscoverer{
		assignmentWatcher: logCoordManager,
		lastDiscovery:     nil,
	}
}

// channelAssignmentDiscoverer is the discoverer for channel assignment.
type channelAssignmentDiscoverer struct {
	assignmentWatcher types.AssignmentDiscoverWatcher // last discovered state and last version discovery.
	lastDiscovery     *types.VersionedStreamingNodeAssignments
}

// NewVersionedState returns a lowest versioned state.
func (d *channelAssignmentDiscoverer) NewVersionedState() VersionedState {
	return VersionedState{
		Version: typeutil.VersionInt64Pair{Global: -1, Local: -1},
		State:   resolver.State{},
	}
}

// channelAssignmentDiscoverer implements the resolver.Discoverer interface.
func (d *channelAssignmentDiscoverer) Discover(ctx context.Context, cb func(VersionedState) error) error {
	if d.lastDiscovery != nil {
		// Always send the current state first if there's.
		// Outside logic may lost the last state before retry Discover function.
		if err := cb(d.parseState()); err != nil {
			return err
		}
	}
	return d.assignmentWatcher.AssignmentDiscover(ctx, func(assignments *types.VersionedStreamingNodeAssignments) error {
		d.lastDiscovery = assignments
		return cb(d.parseState())
	})
}

// parseState parses the addresses from the discovery response.
// Always perform a copy here.
func (d *channelAssignmentDiscoverer) parseState() VersionedState {
	addrs := make([]resolver.Address, 0, len(d.lastDiscovery.Assignments))
	for _, assignment := range d.lastDiscovery.Assignments {
		assignment := assignment
		addrs = append(addrs, resolver.Address{
			Addr: assignment.NodeInfo.Address,
			// resolverAttributes is important to use when resolving, server id to make resolver.Address with same adresss different.
			Attributes: attributes.WithServerID(new(attributes.Attributes), assignment.NodeInfo.ServerID),
			// balancerAttributes can be seen by picker of grpc balancer.
			BalancerAttributes: attributes.WithChannelAssignmentInfo(new(attributes.Attributes), &assignment),
		})
	}
	// TODO: service config should be sent by resolver in future to achieve dynamic configuration for grpc.
	return VersionedState{
		Version: d.lastDiscovery.Version,
		State:   resolver.State{Addresses: addrs},
	}
}

// ChannelAssignmentInfo returns the channel assignment info from the resolver state.
func (s *VersionedState) ChannelAssignmentInfo() map[int64]types.StreamingNodeAssignment {
	assignments := make(map[int64]types.StreamingNodeAssignment)
	for _, v := range s.State.Addresses {
		assignment := attributes.GetChannelAssignmentInfoFromAttributes(v.BalancerAttributes)
		if assignment == nil {
			log.Error("no assignment found in resolver state, skip it", zap.String("address", v.Addr))
			continue
		}
		assignments[assignment.NodeInfo.ServerID] = *assignment
	}
	return assignments
}
