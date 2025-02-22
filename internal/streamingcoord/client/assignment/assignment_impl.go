package assignment

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewAssignmentService creates a new assignment service.
func NewAssignmentService(service lazygrpc.Service[streamingpb.StreamingCoordAssignmentServiceClient]) *AssignmentServiceImpl {
	ctx, cancel := context.WithCancel(context.Background())
	s := &AssignmentServiceImpl{
		ctx:            ctx,
		cancel:         cancel,
		lifetime:       typeutil.NewLifetime(),
		watcher:        newWatcher(),
		service:        service,
		resumingExitCh: make(chan struct{}),
		cond:           syncutil.NewContextCond(&sync.Mutex{}),
		discoverer:     nil,
		logger:         log.With(),
	}
	go s.resumeLoop()
	return s
}

type AssignmentServiceImpl struct {
	ctx            context.Context
	cancel         context.CancelFunc
	lifetime       *typeutil.Lifetime
	watcher        *watcher
	service        lazygrpc.Service[streamingpb.StreamingCoordAssignmentServiceClient]
	resumingExitCh chan struct{}
	cond           *syncutil.ContextCond
	discoverer     *assignmentDiscoverClient
	logger         *log.MLogger
}

// AssignmentDiscover watches the assignment discovery.
func (c *AssignmentServiceImpl) AssignmentDiscover(ctx context.Context, cb func(*types.VersionedStreamingNodeAssignments) error) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("assignment service client is closing")
	}
	defer c.lifetime.Done()

	return c.watcher.AssignmentDiscover(ctx, cb)
}

// ReportAssignmentError reports the assignment error to server.
func (c *AssignmentServiceImpl) ReportAssignmentError(ctx context.Context, pchannel types.PChannelInfo, assignmentErr error) error {
	if !c.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("assignment service client is closing")
	}
	defer c.lifetime.Done()

	// wait for service ready.
	assignment, err := c.getAssignmentDiscoverOrWait(ctx)
	if err != nil {
		return errors.Wrap(err, "at creating assignment service")
	}
	assignment.ReportAssignmentError(pchannel, assignmentErr)
	return nil
}

// Close closes the assignment service.
func (c *AssignmentServiceImpl) Close() {
	c.lifetime.SetState(typeutil.LifetimeStateStopped)
	c.lifetime.Wait()

	c.cancel()
	<-c.resumingExitCh
	c.cond.L.Lock()
	if c.discoverer != nil {
		c.discoverer.Close()
	}
	c.cond.L.Unlock()
}

// getProducerOrWaitProducerReady get producer or wait the new producer is available.
func (c *AssignmentServiceImpl) getAssignmentDiscoverOrWait(ctx context.Context) (*assignmentDiscoverClient, error) {
	c.cond.L.Lock()
	for c.discoverer == nil || !c.discoverer.IsAvailable() {
		if err := c.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}
	discoverer := c.discoverer
	c.cond.L.Unlock()
	return discoverer, nil
}

func (c *AssignmentServiceImpl) resumeLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("stop resuming", zap.Error(err))
		} else {
			c.logger.Info("stop resuming")
		}
		close(c.resumingExitCh)
	}()

	for {
		// Do a underlying assignmentDiscoverClient swap
		adc, err := c.swapAssignmentDiscoverClient()
		if err != nil {
			return err
		}
		if err := c.waitUntilUnavailable(adc); err != nil {
			return err
		}
	}
}

// swapAssignmentDiscoverClient swaps the assignment discover client.
func (c *AssignmentServiceImpl) swapAssignmentDiscoverClient() (*assignmentDiscoverClient, error) {
	adc, err := c.createNewAssignmentDiscoverClient()
	if err != nil {
		return nil, err
	}
	c.cond.LockAndBroadcast()
	oldADC := c.discoverer
	c.discoverer = adc
	c.cond.L.Unlock()

	c.logger.Info("swap assignment discover client")
	if oldADC != nil {
		oldADC.Close()
	}
	c.logger.Info("old assignment discover client closed")
	return adc, nil
}

// getAssignmentDiscoverClient returns the assignment discover client.
func (c *AssignmentServiceImpl) createNewAssignmentDiscoverClient() (*assignmentDiscoverClient, error) {
	for {
		// Create a new available assignment discover client.
		service, err := c.service.GetService(c.ctx)
		if err != nil {
			return nil, err
		}
		client, err := service.AssignmentDiscover(c.ctx)
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if err != nil {
			c.logger.Warn("create a assignment discover stream failed", zap.Error(err))
			// TODO: backoff
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return newAssignmentDiscoverClient(c.watcher, client), nil
	}
}

func (c *AssignmentServiceImpl) waitUntilUnavailable(adc *assignmentDiscoverClient) error {
	select {
	case <-adc.Available():
		c.logger.Warn("assignment discover client is unavailable, try to resuming...")
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}
