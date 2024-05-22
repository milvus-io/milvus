package discoverer

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"google.golang.org/grpc/resolver"
)

type AssignmentDiscoverWatcher interface {
	// AssignmentDiscover watches the assignment discovery.
	// The callback will be called when the discovery is changed.
	// The final error will be returned when the watcher is closed or broken.
	AssignmentDiscover(ctx context.Context, cb func(*logpb.AssignmentDiscoverResponse) error) error
}

// channelAssignmentDiscoverer is the discoverer for channel assignment.
type channelAssignmentDiscoverer struct {
	assignmentWatcher AssignmentDiscoverWatcher
	// last discovered state and last version discovery.
	lastDiscovery *logpb.AssignmentDiscoverResponse
}

func (d *channelAssignmentDiscoverer) NewVersionedState() VersionedState {
	return VersionedState{
		Version: util.NewVersionInt64Pair(),
		State:   resolver.State{},
	}
}

// channelAssignmentDiscoverer implements the resolver.Discoverer interface.
func (d *channelAssignmentDiscoverer) Discover(ctx context.Context, cb func(VersionedState) error) error {
	// Always send the current state first.
	// Outside logic may lost the last state before retry Discover function.
	if err := cb(d.parseState()); err != nil {
		return err
	}
	return d.assignmentWatcher.AssignmentDiscover(ctx, func(resp *logpb.AssignmentDiscoverResponse) error {
		d.lastDiscovery = resp
		return cb(d.parseState())
	})
}

// parseState parses the addresses from the discovery response.
// Always perform a copy here.
func (d *channelAssignmentDiscoverer) parseState() VersionedState {
	if d.lastDiscovery == nil {
		return d.NewVersionedState()
	}

	addrs := make([]resolver.Address, 0, len(d.lastDiscovery.Addresses))
	for _, addr := range d.lastDiscovery.Addresses {
		attr := new(attributes.Attributes)
		channels := make(map[string]logpb.PChannelInfo, len(addr.Channels))
		for _, ch := range addr.Channels {
			channels[ch.Name] = *ch
		}
		attr = attributes.WithChannelAssignmentInfo(attr, addr)
		addrs = append(addrs, resolver.Address{
			Addr:               addr.Address,
			BalancerAttributes: attr,
		})
	}
	return VersionedState{
		Version: &util.VersionInt64Pair{
			Global: d.lastDiscovery.Version.Global,
			Local:  d.lastDiscovery.Version.Local,
		},
		State: resolver.State{
			Addresses: addrs,
		},
	}
}
