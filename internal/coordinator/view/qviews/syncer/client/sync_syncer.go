package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"go.uber.org/zap"
)

var _ QueryViewServiceSyncer = (*grpcSyncer)(nil)

// newGRPCSyncer creates a sync client.
func newGRPCSyncer(client *queryViewServiceClientImpl, req *SyncOption, logger *log.MLogger) *grpcSyncer {
	syncer := &grpcSyncer{
		notifier:   syncutil.NewAsyncTaskNotifier[struct{}](),
		workNode:   req.WorkNode,
		sendCh:     make(chan *viewpb.SyncQueryViewsRequest, 10),
		receiver:   req.Receiver,
		sendExitCh: make(chan struct{}),
		recvExitCh: make(chan struct{}),
	}
	syncer.SetLogger(logger)
	go syncer.execute(client)
	return syncer
}

// grpcSyncer is the client wrapper of sync stream rpc for sync service.
type grpcSyncer struct {
	log.Binder

	notifier   *syncutil.AsyncTaskNotifier[struct{}]
	workNode   qviews.WorkNode
	sendCh     chan *viewpb.SyncQueryViewsRequest
	receiver   chan<- SyncMessage
	sendExitCh chan struct{}
	recvExitCh chan struct{}
}

// SyncAtBackground creates a sync stream rpc client.
// This operation doesn't promise the sync operation is done at server-side.
// Make sure the sync operation is done by the Receiving message.
func (c *grpcSyncer) SyncAtBackground(req *viewpb.SyncQueryViewsRequest) {
	select {
	case <-c.notifier.Context().Done():
	case <-c.sendExitCh:
	case c.sendCh <- req:
	}
}

// Close close the client.
func (c *grpcSyncer) Close() {
	c.notifier.Cancel()
	c.notifier.BlockUntilFinish()
}

func (c *grpcSyncer) execute(qvsc *queryViewServiceClientImpl) (err error) {
	defer func() {
		c.notifier.Finish(struct{}{})
		if err != nil && !errors.Is(err, context.Canceled) {
			select {
			case c.receiver <- SyncErrorMessage{WorkNode: c.workNode, Error: err}:
			case <-c.notifier.Context().Done():
				// If the syncer is closed actively, the error message can be dropped.
			}
		}
		c.Logger().Info("grpc view syncer stopped")
	}()

	c.Logger().Info("try to start new grpc view syncer...")

	// Get the related service and target node id by worknode.
	// maybe a streamingnode or querynode.
	streamClient, err := qvsc.createNewSyncStreamClient(c.notifier.Context(), c.workNode)
	if err != nil {
		return err
	}
	newStreamClient := syncGrpcClient{streamClient}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	recvArmClosed := make(chan struct{}, 1)
	go func() {
		defer close(recvArmClosed)
		c.recvLoop(newStreamClient)
	}()

	c.Logger().Info("new grpc view syncer on working")
	sendErr := c.sendLoop(newStreamClient)
	<-recvArmClosed
	return sendErr
}

// sendLoop sends the produce message to server.
func (c *grpcSyncer) sendLoop(syncGrpcClient syncGrpcClient) (err error) {
	defer func() {
		if err != nil {
			c.Logger().Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			c.Logger().Info("send arm of stream closed")
		}
		if err := syncGrpcClient.CloseSend(); err != nil {
			// If the close send fail, the recv arm should be closed because grpc stream broken.
			c.Logger().Warn("failed to close send", zap.Error(err))
		}
		close(c.sendExitCh)
	}()

	for {
		select {
		case <-c.notifier.Context().Done():
			// The only one way to normal close the stream is the context done.
			return syncGrpcClient.SendClose()
		case <-c.recvExitCh:
			return errors.New("recv arm of stream closed")
		case req, ok := <-c.sendCh:
			if !ok {
				panic("send channel should never be closed")
			}
			if err := syncGrpcClient.SendViews(req); err != nil {
				return err
			}
		}
	}
}

// recvLoop receives the response from server.
func (c *grpcSyncer) recvLoop(syncGrpcClient syncGrpcClient) (err error) {
	skippedAfterClosed := 0
	receiver := c.receiver
	defer func() {
		if err != nil {
			c.Logger().Warn("recv arm of stream closed by unexpected error", zap.Int("skippedAfterClosed", skippedAfterClosed), zap.Error(err))
		} else {
			c.Logger().Info("recv arm of stream closed", zap.Int("skippedAfterClosed", skippedAfterClosed))
		}
		close(c.recvExitCh)
	}()

	for {
		resp, err := syncGrpcClient.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *viewpb.SyncResponse_Close:
			// recv io.EOF after this message.
		case *viewpb.SyncResponse_Views:
			select {
			case <-c.notifier.Context().Done():
				// The grpc syncer is on closing, so the receiver message should be dropped after that.
				receiver = nil
				skippedAfterClosed++
			case receiver <- SyncResponseMessage{
				WorkNode: c.workNode,
				Response: resp.Views,
			}:
			}
		default:
			// skip message here.
			c.Logger().Error("unknown response type", zap.Any("response", resp))
		}
	}
}
