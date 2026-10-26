package replicadiscovery

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Serve owns all sends. Requests and directory notifications never send from
// callbacks, and the directory never waits for RPC progress.
func Serve(stream viewpb.ViewQueryService_WatchVChannelReplicasServer, pchannel string, watch *Watch, unavailable <-chan struct{}) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	// Returning from the RPC cancels a flow-controlled Send/Recv. Never wait
	// for either operation before allowing the handler to return on WAL shutdown.
	result := make(chan error, 1)
	go func() { result <- serve(ctx, stream, pchannel, watch) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-unavailable:
		return status.NewOnShutdownError("replica directory WAL unavailable")
	case <-watch.Done():
		return status.NewOnShutdownError("replica directory WAL closed")
	case err := <-result:
		return err
	}
}

func serve(ctx context.Context, stream viewpb.ViewQueryService_WatchVChannelReplicasServer, pchannel string, watch *Watch) error {
	if err := stream.Send(&viewpb.WatchVChannelReplicasResponse{Response: &viewpb.WatchVChannelReplicasResponse_Ready{Ready: true}}); err != nil {
		return err
	}
	requests := make(chan *viewpb.WatchVChannelReplicasRequest)
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
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
		case <-watch.Done():
			return status.NewOnShutdownError("replica directory WAL closed")
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case req := <-requests:
			if req.GetVchannel() == "" || funcutil.ToPhysicalChannel(req.GetVchannel()) != pchannel {
				return status.NewInvalidRequestSeq("replica watch VChannel does not belong to this PChannel")
			}
			if req.GetUnsubscribe() {
				watch.Unsubscribe(req.GetVchannel())
				if err := stream.Send(&viewpb.WatchVChannelReplicasResponse{Response: &viewpb.WatchVChannelReplicasResponse_UnsubscribedVchannel{UnsubscribedVchannel: req.GetVchannel()}}); err != nil {
					return err
				}
			} else {
				watch.Subscribe(req.GetVchannel())
			}
		case <-watch.Changed():
			if snapshot := watch.Pop(); snapshot != nil {
				if err := stream.Send(&viewpb.WatchVChannelReplicasResponse{Response: &viewpb.WatchVChannelReplicasResponse_Snapshot{Snapshot: snapshot}}); err != nil {
					return err
				}
			}
		}
	}
}
