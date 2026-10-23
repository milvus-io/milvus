package handler

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ViewSyncServer implements the server side of the ViewSyncService.SyncQueryView
// bidirectional streaming RPC. It receives query views from the coordinator,
// delegates them to a QueryViewHandler, and sends back sync reports.
type ViewSyncServer struct {
	viewpb.UnimplementedViewSyncServiceServer
	handler QueryViewHandler
}

// NewViewSyncServer creates a new ViewSyncServer with the given handler.
func NewViewSyncServer(handler QueryViewHandler) *ViewSyncServer {
	return &ViewSyncServer{handler: handler}
}

// SyncQueryView implements viewpb.ViewSyncServiceServer.
// It runs a recv loop in the current goroutine and a send loop in a background goroutine.
// All stream.Send calls are serialized through the send loop to ensure thread safety.
func (s *ViewSyncServer) SyncQueryView(stream viewpb.ViewSyncService_SyncQueryViewServer) error {
	return s.SyncQueryViewUntil(stream, nil)
}

// SyncQueryViewUntil runs SyncQueryView and sends a close response when closeSignal
// is closed. The client is expected to close its send side after receiving the
// close response so the server recv loop can exit.
func (s *ViewSyncServer) SyncQueryViewUntil(stream viewpb.ViewSyncService_SyncQueryViewServer, closeSignal <-chan struct{}) error {
	ctx := stream.Context()
	pending := newPendingReports()

	closeWatcherDone := make(chan struct{})
	if closeSignal != nil {
		go func() {
			select {
			case <-closeSignal:
				pending.SetCloseResponse()
			case <-closeWatcherDone:
			case <-ctx.Done():
			}
		}()
	}
	defer close(closeWatcherDone)

	// Start send loop goroutine — the only goroutine that calls stream.Send.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		s.sendLoop(ctx, stream, pending)
	}()

	// Recv loop: receive views from coord, apply them, update pending reports.
	err := s.recvLoop(stream, pending)

	// Close pending to unblock the send loop so it can drain and exit.
	pending.Close()
	<-sendDone
	return err
}

// sendLoop is the only goroutine that calls stream.Send.
// It waits for pending reports to become available and sends them.
// Exits when ctx is canceled, a close response is sent, or pending is closed.
func (s *ViewSyncServer) sendLoop(
	ctx context.Context,
	stream viewpb.ViewSyncService_SyncQueryViewServer,
	pending *pendingReports,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-pending.Ready():
			// Drain pending reports regardless of whether the channel is
			// still open or was closed (final drain before exit).
			closing, err := s.sendPendingReports(stream, pending)
			if err != nil || closing || !ok {
				return
			}
		}
	}
}

// sendPendingReports drains all pending reports and sends them on the stream.
// Returns closing=true if a close response was sent.
func (s *ViewSyncServer) sendPendingReports(
	stream viewpb.ViewSyncService_SyncQueryViewServer,
	pending *pendingReports,
) (closing bool, err error) {
	protos, closing := pending.Drain()
	if len(protos) > 0 {
		if err := stream.Send(&viewpb.SyncResponse{
			Response: &viewpb.SyncResponse_Views{
				Views: &viewpb.SyncQueryViewsResponse{
					QueryViews: protos,
				},
			},
		}); err != nil {
			mlog.Warn(stream.Context(), "ViewSyncServer: failed to send reports", mlog.Err(err))
			return false, err
		}
	}
	if closing {
		if err := stream.Send(&viewpb.SyncResponse{
			Response: &viewpb.SyncResponse_Close{
				Close: &viewpb.SyncCloseResponse{},
			},
		}); err != nil {
			mlog.Warn(stream.Context(), "ViewSyncServer: failed to send close response", mlog.Err(err))
			return false, err
		}
	}
	return closing, nil
}

// recvLoop receives SyncRequests from the coordinator and processes them.
func (s *ViewSyncServer) recvLoop(
	stream viewpb.ViewSyncService_SyncQueryViewServer,
	pending *pendingReports,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			mlog.Warn(stream.Context(), "ViewSyncServer: stream recv failed", mlog.Err(err))
			return err
		}

		switch r := req.GetRequest().(type) {
		case *viewpb.SyncRequest_Views:
			s.handleViewsRequest(pending, r.Views)
		case *viewpb.SyncRequest_Close:
			// Graceful close: write close response through pending so
			// the send loop serializes it after any buffered reports.
			pending.SetCloseResponse()
			return nil
		}
	}
}

// handleViewsRequest processes a SyncQueryViewsRequest: converts protos to ApplyViews,
// calls the handler, and updates pending reports.
func (s *ViewSyncServer) handleViewsRequest(
	pending *pendingReports,
	viewsReq *viewpb.SyncQueryViewsRequest,
) {
	applyViews := make([]ApplyView, 0, len(viewsReq.QueryViews))
	for _, pb := range viewsReq.QueryViews {
		view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
		applyViews = append(applyViews, ApplyView{
			View: view,
			OnReport: func(report qviews.QueryViewAtWorkNode) {
				// Non-blocking: updates the latest state for this view key
				// and signals the send loop.
				pending.Update(report)
			},
		})
	}

	s.handler.ApplyViews(applyViews)
}
