package snmanager

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	latestAssignments   map[string]types.PChannelInfoAssigned               // The latest assignments info got from streaming coord balance module.
	nodeChangedNotifier *syncutil.VersionedNotifier                         // used to notify that node in streaming node manager has been changed.
	nodesMu             sync.Mutex                                          // protects previousNodesByRG from concurrent access.
	previousNodesByRG   map[int64]*types.StreamingNodeInfoWithResourceGroup // used to store the previous nodes by resource group.
}

// GetBalancer returns the balancer of the streaming node manager.
func (s *StreamingNodeManager) GetBalancer() balancer.Balancer {
	b, err := balance.GetWithContext(context.Background())
	if err != nil {
		panic(err)
	}
	return b
}

// AllocVirtualChannels allocates virtual channels for a collection.
func (s *StreamingNodeManager) AllocVirtualChannels(ctx context.Context, param balancer.AllocVChannelParam) ([]string, error) {
	balancer, err := balance.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return balancer.AllocVirtualChannels(ctx, param)
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
		return -1, merr.WrapErrChannelNotFound(vchannel)
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
//
// Every streaming node is returned, including one that declared it does not
// serve shard queries: this answers "which nodes own a write ahead log", which
// is what the callers that count streaming nodes or dispatch a request to one
// are asking. Placing a shard delegator is a different question, and
// GetStreamingQueryNodeIDsByResourceGroup is what answers it.
func (s *StreamingNodeManager) GetStreamingQueryNodeIDs() typeutil.UniqueSet {
	streamingNodes := s.fetchStreamingNodes()
	streamingNodeIDs := typeutil.NewUniqueSet()
	for _, streamingNode := range streamingNodes {
		streamingNodeIDs.Insert(streamingNode.ServerID)
	}
	return streamingNodeIDs
}

// GetStreamingQueryNodeIDsByResourceGroup returns the server ids of the streaming query nodes grouped by resource group.
//
// A streaming node that declared it does not serve shard queries is left out.
// This is the only thing that feeds a replica's streaming query nodes, and a
// replica's streaming query nodes are the only nodes a shard delegator is ever
// placed on, so leaving it out here is what keeps a delegator off it - rather
// than each placement site having to remember to exclude it.
//
// No streaming node declares this unless a deployment labels it, so a stock
// binary groups exactly the nodes it always did.
func (s *StreamingNodeManager) GetStreamingQueryNodeIDsByResourceGroup() map[string]typeutil.UniqueSet {
	streamingNodes := s.fetchStreamingNodes()
	nodesByRG := make(map[string]typeutil.UniqueSet)
	for _, node := range streamingNodes {
		if node.NoQueryService {
			continue
		}
		if _, ok := nodesByRG[node.ResourceGroup]; !ok {
			nodesByRG[node.ResourceGroup] = typeutil.NewUniqueSet()
		}
		nodesByRG[node.ResourceGroup].Insert(node.ServerID)
	}
	return nodesByRG
}

// NoQueryServiceResourceGroups returns the resource groups whose streaming
// nodes ALL declare no-query-service - at least one node, none serving. This
// is the positive signal the checkers' delegator fallback keys on: "this
// resource group's delegators belong on regular query nodes by declaration"
// is a different fact from "the streaming-query-node set happens to be empty
// right now", which is what a streaming node mid-restart looks like.
// StreamingNodeResourceGroups returns every resource group that currently has
// at least one streaming node, whatever it declares. The complement is how a
// caller tells "this resource group has no streaming nodes at all" from "its
// streaming nodes declare no-query-service" - two facts with different
// consequences for delegator placement.
func (s *StreamingNodeManager) StreamingNodeResourceGroups() typeutil.Set[string] {
	streamingNodes := s.fetchStreamingNodes()
	sawRG := typeutil.NewSet[string]()
	for _, node := range streamingNodes {
		sawRG.Insert(node.ResourceGroup)
	}
	return sawRG
}

func (s *StreamingNodeManager) NoQueryServiceResourceGroups() typeutil.Set[string] {
	streamingNodes := s.fetchStreamingNodes()
	sawRG := typeutil.NewSet[string]()
	servingRG := typeutil.NewSet[string]()
	for _, node := range streamingNodes {
		sawRG.Insert(node.ResourceGroup)
		if !node.NoQueryService {
			servingRG.Insert(node.ResourceGroup)
		}
	}
	return sawRG.Complement(servingRG)
}

// fetchStreamingNodes fetches all streaming nodes from balancer, falling back to cached nodes on error.
// The result is cached for use during shutdown when the balancer may not be available.
func (s *StreamingNodeManager) fetchStreamingNodes() map[int64]*types.StreamingNodeInfoWithResourceGroup {
	balancer, err := balance.GetWithContext(context.Background())
	if err != nil {
		panic(err)
	}
	streamingNodes, err := balancer.GetAvailableStreamingNodes(context.Background())

	s.nodesMu.Lock()
	defer s.nodesMu.Unlock()
	if err != nil {
		// when the streaming coord is on shutdown, the balancer will return an error,
		// causing panic, so we need to return the previous nodes.
		streamingNodes = s.previousNodesByRG
	}
	// Deep copy into cache to prevent callers from mutating the cached map.
	s.previousNodesByRG = make(map[int64]*types.StreamingNodeInfoWithResourceGroup, len(streamingNodes))
	for k, v := range streamingNodes {
		copied := *v
		s.previousNodesByRG[k] = &copied
	}
	return streamingNodes
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
			mlog.Info(context.TODO(), "streaming node manager updated", mlog.Any("assignments", s.latestAssignments))
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
