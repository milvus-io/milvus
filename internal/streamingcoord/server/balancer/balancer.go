package balancer

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_                 Balancer = (*balancerImpl)(nil)
	ErrBalancerClosed          = errors.New("balancer is closed")
)

// Balancer is a load balancer to balance the load of log node.
// Given the balance result to assign or remove channels to corresponding log node.
// Balancer is a local component, it should promise all channel can be assigned, and reach the final consistency.
// Balancer should be thread safe.
type Balancer interface {
	// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
	GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool)

	// WatchChannelAssignments watches the balance result.
	WatchChannelAssignments(ctx context.Context, cb func(version typeutil.VersionInt64Pair, relations []types.PChannelInfoAssigned) error) error

	// MarkAsAvailable marks the pchannels as available, and trigger a rebalance.
	MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error

	// Trigger is a hint to trigger a balance.
	Trigger(ctx context.Context) error

	// Close close the balancer.
	Close()
}
