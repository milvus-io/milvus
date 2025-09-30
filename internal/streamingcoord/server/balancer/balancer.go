package balancer

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	_                 Balancer = (*balancerImpl)(nil)
	ErrBalancerClosed          = errors.New("balancer is closed")
)

type (
	WatchChannelAssignmentsCallbackParam = channel.WatchChannelAssignmentsCallbackParam
	WatchChannelAssignmentsCallback      = channel.WatchChannelAssignmentsCallback
)

// Balancer is a load balancer to balance the load of log node.
// Given the balance result to assign or remove channels to corresponding log node.
// Balancer is a local component, it should promise all channel can be assigned, and reach the final consistency.
// Balancer should be thread safe.
type Balancer interface {
	// GetLatestChannelAssignment returns the latest channel assignment.
	GetLatestChannelAssignment() (*WatchChannelAssignmentsCallbackParam, error)

	// GetAllStreamingNodes fetches all streaming node info.
	GetAllStreamingNodes(ctx context.Context) (map[int64]*types.StreamingNodeInfo, error)

	// UpdateBalancePolicy update the balance policy.
	UpdateBalancePolicy(ctx context.Context, req *streamingpb.UpdateWALBalancePolicyRequest) (*streamingpb.UpdateWALBalancePolicyResponse, error)

	// ReplicateRole returns the replicate role of the balancer.
	ReplicateRole() replicateutil.Role

	// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
	// If the error is returned, the balancer is closed.
	// Otherwise, the following rules are applied:
	// 1. If the streaming service already enabled once (write is applied), the notifier will be notified before this function returns.
	// 2. If the streaming service is not already enabled,
	// 	  the notifier will be notified when the streaming service can be enabled (all node in cluster is upgrading to 2.6)
	//    and the balancer will wait for all notifier is finish, and then start the streaming service.
	// 3. The caller should call the notifier finish method, after the caller see notification and finish its work.
	RegisterStreamingEnabledNotifier(notifier *syncutil.AsyncTaskNotifier[struct{}])

	// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
	GetLatestWALLocated(ctx context.Context, pchannel string) (int64, bool)

	// WatchChannelAssignments watches the balance result.
	WatchChannelAssignments(ctx context.Context, cb WatchChannelAssignmentsCallback) error

	// MarkAsAvailable marks the pchannels as available, and trigger a rebalance.
	MarkAsUnavailable(ctx context.Context, pChannels []types.PChannelInfo) error

	// UpdateReplicateConfiguration updates the replicate configuration.
	UpdateReplicateConfiguration(ctx context.Context, msgs ...message.ImmutableAlterReplicateConfigMessageV2) error

	// Trigger is a hint to trigger a balance.
	Trigger(ctx context.Context) error

	// Close close the balancer.
	Close()
}
