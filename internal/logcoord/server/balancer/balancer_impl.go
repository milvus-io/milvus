package balancer

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/logcoord/server/channel"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/layout"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// NewBalancer create a balancer.
func NewBalancer(policy Policy,
	layout *layout.Layout,
	assignmentHelper AssignmentHelper,
) Balancer {
	b := &balancerImpl{
		lifetime:         lifetime.NewLifetime(lifetime.Working),
		policy:           policy,
		layout:           layout,
		watcher:          newNodeStatusWatcher(layout),
		assignmentHelper: assignmentHelper,
		req:              make(chan interface{}, 5),
		closed:           make(chan struct{}),
		finished:         make(chan struct{}),
	}
	go b.execute()
	return b
}

// opUpdateChannel is a operation to update channel to log meta.
type opUpdateChannel struct {
	channel  map[string]channel.PhysicalChannel
	finished chan struct{}
}

// opUpdateLogNodeStatus is a operation to update log node status.
type opUpdateLogNodeStatus struct {
	status   map[int64]*layout.NodeStatus
	finished chan struct{}
}

// opReBalance is a operation to trigger a re-balance operation.
type opReBalance struct {
	finished chan struct{}
}

// balancerImpl is a implementation of Balancer.
type balancerImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	// used to background balance.
	policy Policy
	// used to store the latest layout.
	layout *layout.Layout
	// used to notify the layout change.
	watcher *nodeStatusWatcher
	// used to apply assign operations.
	assignmentHelper AssignmentHelper
	req              chan interface{}
	closed           chan struct{}
	finished         chan struct{}
}

// WatchBalanceResult watches the balance result.
func (b *balancerImpl) WatchBalanceResult(ctx context.Context, cb func(version *util.VersionInt64Pair, nodeStatus map[int64]*layout.NodeStatus) error) error {
	if b.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	return b.watcher.WatchBalanceResult(ctx, cb)
}

// ReBalance trigger a re-balance.
func (b *balancerImpl) ReBalance() error {
	if b.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	req := &opReBalance{
		finished: make(chan struct{}, 1),
	}
	b.req <- req
	return nil
}

// UpdateChannel update the channel to balancer.
func (b *balancerImpl) UpdateChannel(channels map[string]channel.PhysicalChannel) error {
	if b.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	req := &opUpdateChannel{
		channel:  channels,
		finished: make(chan struct{}, 1),
	}
	b.req <- req
	return nil
}

// UpdateLogNodeStatus update the log node status.
func (b *balancerImpl) UpdateLogNodeStatus(nodeStatus map[int64]*layout.NodeStatus) error {
	if b.lifetime.Add(lifetime.IsWorking) != nil {
		return status.NewOnShutdownError("balancer is closing")
	}
	defer b.lifetime.Done()

	req := &opUpdateLogNodeStatus{
		status:   nodeStatus,
		finished: make(chan struct{}, 1),
	}
	b.req <- req
	return nil
}

// Close close the balancer.
func (b *balancerImpl) Close() {
	b.lifetime.SetState(lifetime.Stopped)
	b.lifetime.Wait()

	close(b.closed)
	<-b.finished
}

// execute the balancer.
func (b *balancerImpl) execute() {
	defer close(b.finished)

	var delayCh <-chan time.Time
	errCount := 0
	for {
		// execute serialization.
		select {
		case op := <-b.req:
			// handle operations as much as possible.
			b.handlingOperation(op)
		L:
			for {
				select {
				case op := <-b.req:
					b.handlingOperation(op)
				default:
					break L
				}
			}
		case <-delayCh:
		case <-b.closed:
			return
		}

		// TODO: !!! These implementation can not promise the channel assigned to one lognode constraints.
		// A Watch on lognode per channel is needed
		// the new balance should wait for heartbeat timeout if remove operation failed to avoid old lognode still in using.

		// execute parallel.
		if err := b.balance(); err != nil {
			// TODO: configurable
			// if balance failed, we will retry it or wait for next balance status updates.
			after := 100 * time.Millisecond * time.Duration(errCount)
			if after > 1*time.Second {
				after = 1 * time.Second
			}
			log.Warn("fail to apply balance", zap.Duration("after", after), zap.Error(err))
			delayCh = time.After(after)
			errCount++
		} else {
			errCount = 0
			delayCh = nil
		}

		// notify all watcher.
		b.watcher.UpdateBalanceResultAndNotify(b.layout)
	}
}

// Trigger a balance of layout.
// Return a nil chan to avoid
// Return a channel to notify the balance trigger again.
func (b *balancerImpl) balance() error {
	// execute policy to get balance operations.
	log.Info(
		"start to balance layout...",
		zap.Any("layout", b.layout),
	)
	builder := newBalanceOPBuilder(b.assignmentHelper, b.layout)
	balanceOPs := b.policy.Balance(builder)

	log.Info("balance operation generated", zap.Int("operationCount", len(balanceOPs)))
	g, _ := errgroup.WithContext(context.Background())

	// Same channel group should be executed sequentially.
	// Different channel group can be executed concurrently.
	for _, ops := range balanceOPs {
		ops := ops
		g.Go(func() error {
			for _, op := range ops {
				// if previous operation failed, we will not continue next.
				if err := op(context.Background()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// handlingOperation handle the operation.
func (b *balancerImpl) handlingOperation(op any) {
	switch op := op.(type) {
	case *opUpdateChannel:
		for name, channelInfo := range op.channel {
			if channelInfo == nil {
				b.layout.DeleteChannel(name)
			} else {
				b.layout.AddChannel(channelInfo)
			}
		}
		close(op.finished)
	case *opUpdateLogNodeStatus:
		b.layout.UpdateNodeStatus(op.status)
		close(op.finished)
	case *opReBalance:
		close(op.finished)
	}
}
