package balancer

import (
	"context"

	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
)

var _ Balancer = (*balancerImpl)(nil)

// Balancer is a load balancer to balance the load of log node.
// Given the balance result to assign or remove channels to corresponding log node.
// Balancer is a local component, it should promise all channel can be assigned, and reach the final consistency.
// Balancer should be thread safe.
type Balancer interface {
	// WatchBalanceResult watches the balance result.
	WatchBalanceResult(ctx context.Context, cb func(v *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error) error

	// A hint to trigger a balance.
	ReBalance() error

	// UpdateChannel update the channel info.
	// It may be trigger a re-balance of channels.
	UpdateChannel(channels map[string]channel.PhysicalChannel) error

	// UpdateLogNodeStatus update the log node status.
	// It may be trigger a re-balance of channels.
	// If the status is marked as Freeze, the balancer will remove the node, and re balance the channel on that node.
	UpdateLogNodeStatus(status map[int64]*layout.NodeStatus) error

	// Close close the balancer.
	Close()
}
