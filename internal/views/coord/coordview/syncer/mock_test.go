//go:build test && dynamic

package syncer

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ---------------------------------------------------------------------------
// mockStream — implements viewpb.ViewSyncService_SyncQueryViewClient
// ---------------------------------------------------------------------------

type mockStream struct {
	ctx     context.Context
	sendCh  chan *viewpb.SyncRequest  // captures what syncer sends
	recvCh  chan *viewpb.SyncResponse // test injects responses
	sendMu  sync.Mutex
	sendErr error // if non-nil, Send returns this immediately
}

func newMockStream(ctx context.Context) *mockStream {
	return &mockStream{
		ctx:    ctx,
		sendCh: make(chan *viewpb.SyncRequest, 100),
		recvCh: make(chan *viewpb.SyncResponse, 100),
	}
}

func (s *mockStream) Send(req *viewpb.SyncRequest) error {
	s.sendMu.Lock()
	err := s.sendErr
	s.sendMu.Unlock()
	if err != nil {
		return err
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sendCh <- req:
		return nil
	}
}

func (s *mockStream) Recv() (*viewpb.SyncResponse, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case resp, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return resp, nil
	}
}

func (s *mockStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockStream) Trailer() metadata.MD         { return nil }
func (s *mockStream) CloseSend() error             { return nil }
func (s *mockStream) Context() context.Context     { return s.ctx }
func (s *mockStream) SendMsg(m interface{}) error  { return nil }
func (s *mockStream) RecvMsg(m interface{}) error  { return nil }

// setSendErr sets the error returned by future Send calls.
func (s *mockStream) setSendErr(err error) {
	s.sendMu.Lock()
	s.sendErr = err
	s.sendMu.Unlock()
}

// collectSent drains all currently buffered SyncRequests from sendCh.
func (s *mockStream) collectSent() []*viewpb.SyncRequest {
	var reqs []*viewpb.SyncRequest
	for {
		select {
		case req := <-s.sendCh:
			reqs = append(reqs, req)
		default:
			return reqs
		}
	}
}

// waitSend waits for at least one SyncRequest to appear on sendCh within timeout.
func (s *mockStream) waitSend(timeout time.Duration) (*viewpb.SyncRequest, bool) {
	select {
	case req := <-s.sendCh:
		return req, true
	case <-time.After(timeout):
		return nil, false
	}
}

// injectResponse sends a response proto into recvCh.
func (s *mockStream) injectResponse(views ...*viewpb.QueryViewOfShard) {
	s.recvCh <- &viewpb.SyncResponse{
		Response: &viewpb.SyncResponse_Views{
			Views: &viewpb.SyncQueryViewsResponse{
				QueryViews: views,
			},
		},
	}
}

// ---------------------------------------------------------------------------
// mockViewSyncClient — implements ViewSyncClient
// ---------------------------------------------------------------------------

type mockViewSyncClient struct {
	mu           sync.Mutex
	watchCh      chan struct{}
	nodes        map[qviews.WorkNodeKey]qviews.WorkNode
	aliveNodes   map[qviews.WorkNodeKey]bool
	openStreamFn func(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error)
	closed       bool
}

func newMockViewSyncClient() *mockViewSyncClient {
	return &mockViewSyncClient{
		watchCh:    make(chan struct{}, 10),
		nodes:      make(map[qviews.WorkNodeKey]qviews.WorkNode),
		aliveNodes: make(map[qviews.WorkNodeKey]bool),
	}
}

func (c *mockViewSyncClient) WatchNodeChanged(ctx context.Context) (<-chan struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watchCh, nil
}

func (c *mockViewSyncClient) GetAllNodes(ctx context.Context) (map[qviews.WorkNodeKey]qviews.WorkNode, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[qviews.WorkNodeKey]qviews.WorkNode, len(c.nodes))
	for k, v := range c.nodes {
		result[k] = v
	}
	return result, nil
}

func (c *mockViewSyncClient) IsNodeAlive(_ context.Context, node qviews.WorkNode) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.aliveNodes[node.Key()]
}

func (c *mockViewSyncClient) OpenSyncStream(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	c.mu.Lock()
	fn := c.openStreamFn
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, node)
	}
	return newMockStream(ctx), nil
}

func (c *mockViewSyncClient) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// addNode adds a node to both the node map and alive set.
func (c *mockViewSyncClient) addNode(node qviews.WorkNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[node.Key()] = node
	c.aliveNodes[node.Key()] = true
}

// removeNode removes a node from both maps.
func (c *mockViewSyncClient) removeNode(node qviews.WorkNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, node.Key())
	delete(c.aliveNodes, node.Key())
}

// notifyNodeChanged sends a signal to the watch channel.
func (c *mockViewSyncClient) notifyNodeChanged() {
	c.watchCh <- struct{}{}
}

// ---------------------------------------------------------------------------
// Test helpers — view construction
// ---------------------------------------------------------------------------

const (
	testCollectionID int64 = 100
	testReplicaID    int64 = 1
	testVChannel           = "v0_c0"
)

// newTestQNView creates a QueryViewAtQueryNode for the given query node and version.
func newTestQNView(nodeID int64, version int64) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
	qnView := &viewpb.QueryViewOfQueryNode{
		NodeId: nodeID,
		Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{1000 + nodeID}},
		},
	}
	return qviews.NewQueryViewAtQueryNode(meta, qnView)
}

// newTestSNView creates a QueryViewAtStreamingNode for the given version.
func newTestSNView(version int64) qviews.QueryViewAtWorkNode {
	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

// newTestSyncView creates a SyncView for a QN with configurable callbacks.
func newTestSyncView(
	nodeID int64,
	version int64,
	onResp func(qviews.QueryViewAtWorkNode) bool,
	onQueryNodeLost func(qviews.QueryNode),
) SyncView {
	return SyncView{
		View:            newTestQNView(nodeID, version),
		OnSyncResponse:  onResp,
		OnQueryNodeLost: onQueryNodeLost,
	}
}

// newTestSyncGroup creates a SyncGroup from the given SyncViews, auto-grouping by node.
func newTestSyncGroup(views ...SyncView) SyncGroup {
	group := SyncGroup{
		ViewsByNode: make(map[qviews.WorkNodeKey][]SyncView),
	}
	for _, sv := range views {
		key := sv.View.WorkNode().Key()
		group.ViewsByNode[key] = append(group.ViewsByNode[key], sv)
	}
	return group
}

// testTimeUnit is the base time unit for test timeouts.
// Use multiples of this for different scenarios.
const testTimeUnit = 10 * time.Millisecond

// waitFor waits for a channel signal or times out.
func waitFor(ch <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// waitForCond polls a condition function until it returns true or timeout.
func waitForCond(fn func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
