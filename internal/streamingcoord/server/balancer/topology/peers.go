package topology

import (
	"context"
	"maps"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// Peers owns one Sync stream per live registration. Network I/O on one node
// never holds the balance loop or delays the other nodes.
type Peers struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	streams map[int64]*syncPeer
	reports map[int64]*streamingpb.SyncWALReplicasResponse
	open    func(context.Context, int64) (streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasClient, error)
	changed chan struct{}
}
type syncPeer struct {
	cancel context.CancelFunc
	send   chan *streamingpb.SyncWALReplicasRequest
}

func NewPeers(open func(context.Context, int64) (streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasClient, error)) *Peers {
	return &Peers{streams: make(map[int64]*syncPeer), reports: make(map[int64]*streamingpb.SyncWALReplicasResponse), open: open, changed: make(chan struct{}, 1)}
}
func (p *Peers) Changed() <-chan struct{} { return p.changed }
func (p *Peers) Reports() map[int64]*streamingpb.SyncWALReplicasResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	return maps.Clone(p.reports)
}

func (p *Peers) notifyLocked() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (p *Peers) Broadcast(ctx context.Context, live map[int64]*types.StreamingNodeInfoWithResourceGroup, controller *Controller, control *streamingpb.CChannelMeta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, peer := range p.streams {
		if live[id] == nil {
			peer.cancel()
			delete(p.streams, id)
			delete(p.reports, id)
		}
	}
	// Membership confirmation uses only reports on the current control streams.
	req := controller.Request(control, live, p.reports)
	if req == nil {
		return
	}
	for id := range live {
		peer := p.streams[id]
		if peer == nil {
			peerCtx, cancel := context.WithCancel(ctx)
			peer = &syncPeer{cancel: cancel, send: make(chan *streamingpb.SyncWALReplicasRequest, 1)}
			p.streams[id] = peer
			p.wg.Add(1)
			go func() {
				defer cancel()
				p.serve(peerCtx, id, peer)
			}()
		}
		select {
		case <-peer.send:
		default:
		}
		peer.send <- req
	}
}

func (p *Peers) serve(ctx context.Context, id int64, peer *syncPeer) {
	defer p.wg.Done()
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.streams[id] == peer {
			delete(p.streams, id)
			if p.reports[id] != nil {
				delete(p.reports, id)
				p.notifyLocked()
			}
		}
	}()
	stream, err := p.open(ctx, id)
	if err != nil {
		return
	}
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		defer peer.cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-peer.send:
				if err := stream.Send(req); err != nil {
					return
				}
			}
		}
	}()
	defer func() { peer.cancel(); <-sendDone }()
	for {
		report, err := stream.Recv()
		if err != nil {
			if ctx.Err() == nil {
				mlog.Warn(ctx, "WAL replica control stream disconnected", mlog.Int64("nodeID", id), mlog.Err(err))
			}
			return
		}
		p.mu.Lock()
		if p.streams[id] == peer && !proto.Equal(p.reports[id], report) {
			p.reports[id] = report
			p.notifyLocked()
		}
		p.mu.Unlock()
	}
}

func (p *Peers) Close() {
	p.mu.Lock()
	for _, s := range p.streams {
		s.cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
