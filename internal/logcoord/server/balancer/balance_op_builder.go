package balancer

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// AssignmentHelper is a interface to assign or remove channel to log node.
type AssignmentHelper interface {
	// Assign a wal instance for the channel on log node of given server id.
	Assign(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error

	// Remove the wal instance for the channel on log node of given server id.
	Remove(ctx context.Context, serverID int64, channel logpb.PChannelInfo) error
}

// BalanceOP is a operation to balance the load of log node.
type BalanceOP func(ctx context.Context) error

func newBalanceOPBuilder(assignment AssignmentHelper, layout *layout.Layout) *BalanceOPBuilder {
	return &BalanceOPBuilder{
		assignment:   assignment,
		mu:           sync.Mutex{},
		layout:       layout,
		clonedLayout: layout.Clone(),
		ops:          make(map[string][]BalanceOP),
	}
}

// BalanceOPBuilder is a helper to generate BalanceOP for balance layout and find inconsistency in layout.
type BalanceOPBuilder struct {
	assignment   AssignmentHelper
	mu           sync.Mutex
	layout       *layout.Layout
	clonedLayout *layout.Layout

	// Construction results.
	ops map[string][]BalanceOP
}

// GetLayout get the current layout.
func (lh *BalanceOPBuilder) GetLayout() *layout.Layout {
	return lh.clonedLayout
}

func (lh *BalanceOPBuilder) Build() map[string][]BalanceOP {
	return lh.ops
}

// AssignChannel assign the channel from one log node to another.
func (lh *BalanceOPBuilder) AddAssignChannelOP(serverID int64, pChannel channel.PhysicalChannel) {
	info := pChannel.Info()
	info.Term++
	info.ServerID = serverID
	lh.clonedLayout.AssignChannelToNode(serverID, info)

	// find if channel is assigned
	lazyOp := func(ctx context.Context) error {
		newChannel, err := pChannel.Assign(ctx, serverID)
		if err != nil {
			return err
		}

		// Assign the channel to log node.
		if err := lh.assignment.Assign(ctx, serverID, *newChannel); err != nil {
			log.Warn("fail to assign channel", zap.Error(err), zap.Int64("serverID", serverID), zap.Any("channel", newChannel))
			return err
		}
		log.Info("assign channel to log node", zap.Int64("serverID", serverID), zap.Any("channel", newChannel))

		// Update layout.
		lh.mu.Lock()
		lh.layout.AssignChannelToNode(serverID, newChannel)
		lh.mu.Unlock()

		return nil
	}
	name := pChannel.Name()
	lh.ops[name] = append(lh.ops[name], lazyOp)
}

// RemoveChannel remove the channel from log node.
func (lh *BalanceOPBuilder) AddRemoveChannelOP(r layout.Relation) {
	lh.clonedLayout.RemoveChannelFromNode(r.ServerID, r.Channel.Name)

	lazyOp := func(ctx context.Context) error {
		// Remove the channel from log node.
		if err := lh.assignment.Remove(ctx, r.ServerID, *r.Channel); err != nil {
			log.Warn("fail to remove channel", zap.Error(err), zap.Int64("serverID", r.ServerID), zap.Any("channel", r.Channel))
			return err
		}
		log.Info("unassign channel to log node", zap.Int64("serverID", r.ServerID), zap.String("channel", r.Channel.Name))
		// Update layout.
		lh.mu.Lock()
		lh.layout.RemoveChannelFromNode(r.ServerID, r.Channel.Name)
		lh.mu.Unlock()
		return nil
	}
	lh.ops[r.Channel.Name] = append(lh.ops[r.Channel.Name], lazyOp)
}

func (lh *BalanceOPBuilder) OPExist(name string) bool {
	_, ok := lh.ops[name]
	return ok
}
