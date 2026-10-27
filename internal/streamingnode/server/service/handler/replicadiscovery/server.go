package replicadiscovery

import (
	"context"
	"io"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// ServeStream projects the domain interface onto RPC. Snapshot callbacks only
// coalesce pending state, so a slow peer cannot block directory publication.
func ServeStream(rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer, pc string, stream replicadiscovery.Stream) error {
	ctx, cancel := context.WithCancel(rpc.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- run(ctx, rpc, pc, stream) }()
	// Returning releases flow-controlled gRPC sends. Never join Send here.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.Done():
		return stream.Error()
	case err := <-result:
		return err
	}
}

type pendingSnapshots struct {
	mu        sync.Mutex
	snapshots map[int64]*streamingpb.ReplicaWatchSnapshot
	wake      chan struct{}
}

func (p *pendingSnapshots) update(id int64, vc string, snapshot replicadiscovery.Snapshot) {
	replicas := make([]*viewpb.VChannelReplicas_Replica, len(snapshot.Replicas))
	for i, r := range snapshot.Replicas {
		replicas[i] = &viewpb.VChannelReplicas_Replica{ReplicaId: r.ReplicaID, MaxUpViewVersion: r.MaxUpViewVersion.IntoProto()}
	}
	p.mu.Lock()
	p.snapshots[id] = &streamingpb.ReplicaWatchSnapshot{SubscriptionId: id, Snapshot: &viewpb.VChannelReplicas{Vchannel: vc, Replicas: replicas}}
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *pendingSnapshots) pop() *streamingpb.ReplicaWatchSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, snapshot := range p.snapshots {
		delete(p.snapshots, id)
		if len(p.snapshots) > 0 {
			select {
			case p.wake <- struct{}{}:
			default:
			}
		}
		return snapshot
	}
	return nil
}

func run(ctx context.Context, rpc streamingpb.StreamingNodeHandlerService_SubscribeReplicasServer, pc string, stream replicadiscovery.Stream) error {
	if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_CreateStream{CreateStream: &streamingpb.CreateReplicaWatchStreamResponse{}}}); err != nil {
		return err
	}
	pending := &pendingSnapshots{snapshots: make(map[int64]*streamingpb.ReplicaWatchSnapshot), wake: make(chan struct{}, 1)}
	subs := make(map[int64]replicadiscovery.Subscription)
	channels := make(map[int64]string)
	defer func() {
		for _, sub := range subs {
			_ = sub.Close()
		}
	}()
	requests := make(chan *streamingpb.ReplicaWatchRequest)
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := rpc.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case requests <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stream.Done():
			return stream.Error()
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-pending.wake:
			if snapshot := pending.pop(); snapshot != nil {
				if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_Snapshot{Snapshot: snapshot}}); err != nil {
					return err
				}
			}
		case request := <-requests:
			switch req := request.GetRequest().(type) {
			case *streamingpb.ReplicaWatchRequest_Create:
				id, vc := req.Create.GetSubscriptionId(), req.Create.GetVchannel()
				if id <= 0 || subs[id] != nil || vc == "" || funcutil.ToPhysicalChannel(vc) != pc {
					return status.NewInvalidRequestSeq("replica subscription requires a unique positive ID and a VChannel in this PChannel")
				}
				sub, err := stream.Subscribe(ctx, vc, func(snapshot replicadiscovery.Snapshot) { pending.update(id, vc, snapshot) })
				if err != nil {
					if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_SubscriptionError{SubscriptionError: &streamingpb.ReplicaWatchSubscriptionError{SubscriptionId: id, Vchannel: vc, Error: (*streamingpb.StreamingError)(status.AsStreamingError(err))}}}); err != nil {
						return err
					}
					continue
				}
				subs[id] = sub
				channels[id] = vc
				if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_Create{Create: &streamingpb.CreateReplicaWatchSubscriptionResponse{SubscriptionId: id, Vchannel: vc}}}); err != nil {
					return err
				}
			case *streamingpb.ReplicaWatchRequest_CloseSubscription:
				id := req.CloseSubscription.GetSubscriptionId()
				vc := channels[id]
				if sub := subs[id]; sub != nil {
					if err := sub.Close(); err != nil {
						return err
					}
					delete(subs, id)
					delete(channels, id)
				}
				pending.mu.Lock()
				delete(pending.snapshots, id)
				pending.mu.Unlock()
				if err := rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_CloseSubscription{CloseSubscription: &streamingpb.CloseReplicaWatchSubscriptionResponse{SubscriptionId: id, Vchannel: vc}}}); err != nil {
					return err
				}
			case *streamingpb.ReplicaWatchRequest_CloseStream:
				return rpc.Send(&streamingpb.ReplicaWatchResponse{Response: &streamingpb.ReplicaWatchResponse_CloseStream{CloseStream: &streamingpb.CloseReplicaWatchStreamResponse{}}})
			default:
				return status.NewInvalidRequestSeq("unknown replica watch request")
			}
		}
	}
}
