package snmanager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var StaticStreamingNodeManager = newStreamingNodeManager()

var ErrStreamingServiceNotReady = errors.New("streaming service is not ready, may be on-upgrading from old arch")

// TODO: can be removed after streaming service fully manage all growing data.
func newStreamingNodeManager() *StreamingNodeManager {
	snm := &StreamingNodeManager{
		notifier:            syncutil.NewAsyncTaskNotifier[struct{}](),
		cond:                syncutil.NewContextCond(&sync.Mutex{}),
		latestAssignments:   make(map[string]types.PChannelInfoAssigned),
		nodeChangedNotifier: syncutil.NewVersionedNotifier(),
	}
	go snm.execute()
	return snm
}

// NewStreamingReadyNotifier creates a new streaming ready notifier.
func NewStreamingReadyNotifier() *StreamingReadyNotifier {
	return &StreamingReadyNotifier{
		inner: syncutil.NewAsyncTaskNotifier[struct{}](),
	}
}

// StreamingReadyNotifier is a notifier for streaming service ready.
type StreamingReadyNotifier struct {
	inner *syncutil.AsyncTaskNotifier[struct{}]
}

// Release releases the notifier.
func (s *StreamingReadyNotifier) Release() {
	s.inner.Finish(struct{}{})
}

// Ready returns a channel that will be closed when the streaming service is ready.
func (s *StreamingReadyNotifier) Ready() <-chan struct{} {
	return s.inner.Context().Done()
}

// IsReady returns true if the streaming service is ready.
func (s *StreamingReadyNotifier) IsReady() bool {
	return s.inner.Context().Err() != nil
}

// Context returns the context of the notifier.
// StreamingNodeManager is a manager for manage the querynode that embedded into streaming node.
// StreamingNodeManager is exclusive with ResourceManager.
type StreamingNodeManager struct {
	notifier            *syncutil.AsyncTaskNotifier[struct{}]
	cond                *syncutil.ContextCond
	latestAssignments   map[string]types.PChannelInfoAssigned // The latest assignments info got from streaming coord balance module.
	nodeChangedNotifier *syncutil.VersionedNotifier           // used to notify that node in streaming node manager has been changed.
}

// GetBalancer returns the balancer of the streaming node manager.
func (s *StreamingNodeManager) GetBalancer() balancer.Balancer {
	b, err := balance.GetWithContext(context.Background())
	if err != nil {
		panic(err)
	}
	return b
}

// GetLatestWALLocated returns the server id of the node that the wal of the vChannel is located.
// Return -1 and error if the vchannel is not found or context is canceled.
func (s *StreamingNodeManager) GetLatestWALLocated(ctx context.Context, vchannel string) (int64, error) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return -1, err
	}
	serverID, ok := balancer.GetLatestWALLocated(ctx, pchannel)
	if !ok {
		return -1, errors.Errorf("channel: %s not found", vchannel)
	}
	return serverID, nil
}

// CheckIfStreamingServiceReady checks if the streaming service is ready.
func (s *StreamingNodeManager) CheckIfStreamingServiceReady(ctx context.Context) error {
	n := NewStreamingReadyNotifier()
	if err := s.RegisterStreamingEnabledListener(ctx, n); err != nil {
		return err
	}
	defer n.Release()
	if !n.IsReady() {
		// The notifier is not canceled, so the streaming service is not ready.
		return ErrStreamingServiceNotReady
	}
	return nil
}

// RegisterStreamingEnabledNotifier registers a notifier into the balancer.
func (s *StreamingNodeManager) RegisterStreamingEnabledListener(ctx context.Context, notifier *StreamingReadyNotifier) error {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	balancer.RegisterStreamingEnabledNotifier(notifier.inner)
	return nil
}

// GetWALLocated returns the server id of the node that the wal of the vChannel is located.
func (s *StreamingNodeManager) GetWALLocated(vChannel string) int64 {
	pchannel := funcutil.ToPhysicalChannel(vChannel)
	var targetServerID int64

	s.cond.L.Lock()
	for {
		if assignment, ok := s.latestAssignments[pchannel]; ok {
			targetServerID = assignment.Node.ServerID
			break
		}
		s.cond.Wait(context.Background())
	}
	s.cond.L.Unlock()
	return targetServerID
}

// GetStreamingQueryNodeIDs returns the server ids of the streaming query nodes.
func (s *StreamingNodeManager) GetStreamingQueryNodeIDs() typeutil.UniqueSet {
	balancer, err := balance.GetWithContext(context.Background())
	if err != nil {
		panic(err)
	}
	streamingNodes, err := balancer.GetAllStreamingNodes(context.Background())
	if err != nil {
		panic(err)
	}
	streamingNodeIDs := typeutil.NewUniqueSet()
	for _, streamingNode := range streamingNodes {
		streamingNodeIDs.Insert(streamingNode.ServerID)
	}
	return streamingNodeIDs
}

// ListenNodeChanged returns a listener for node changed event.
func (s *StreamingNodeManager) ListenNodeChanged() *syncutil.VersionedListener {
	return s.nodeChangedNotifier.Listen(syncutil.VersionedListenAtEarliest)
}

func (s *StreamingNodeManager) execute() (err error) {
	defer s.notifier.Finish(struct{}{})

	b, err := balance.GetWithContext(s.notifier.Context())
	if err != nil {
		return errors.Wrap(err, "failed to wait balancer ready")
	}
	for {
		if err := b.WatchChannelAssignments(s.notifier.Context(), func(param balancer.WatchChannelAssignmentsCallbackParam) error {
			s.cond.LockAndBroadcast()
			s.latestAssignments = make(map[string]types.PChannelInfoAssigned)
			for _, relation := range param.Relations {
				s.latestAssignments[relation.Channel.Name] = relation
			}
			s.nodeChangedNotifier.NotifyAll()
			log.Info("streaming node manager updated", zap.Any("assignments", s.latestAssignments))
			s.cond.L.Unlock()
			return nil
		}); err != nil {
			return err
		}
	}
}

func (s *StreamingNodeManager) Close() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}
