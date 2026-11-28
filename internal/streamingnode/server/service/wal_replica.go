package service

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type WALReplicaService struct {
	streamingpb.UnimplementedStreamingNodeWALReplicaServiceServer
	manager walmanager.Manager
	control *walmanager.ReplicaControl
}

func NewWALReplicaService(manager walmanager.Manager, control *walmanager.ReplicaControl) *WALReplicaService {
	return &WALReplicaService{manager: manager, control: control}
}

func (s *WALReplicaService) SyncWALReplicas(stream streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	control, err := s.control.Connect()
	if err != nil {
		return err
	}
	defer control.Close()
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)
	requests := make(chan *streamingpb.SyncWALReplicasRequest)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				cancel(err)
				return
			}
			select {
			case requests <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	response, err := control.Sync(first)
	for err == nil {
		if err = stream.Send(response); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), io.EOF) {
				return nil
			}
			return context.Cause(ctx)
		case <-control.Done():
			return status.NewOnShutdownError("WAL replica control stream replaced")
		case req := <-requests:
			response, err = control.Sync(req)
		case <-control.Changed():
			response, err = control.Inventory()
		}
	}
	return err
}

func (s *WALReplicaService) WatchWALReplicaState(stream streamingpb.StreamingNodeWALReplicaService_WatchWALReplicaStateServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open.GetPchannel() == "" || open.GetWriterTerm() <= 0 || open.GetWriterReplicaId() <= 0 || open.GetSubscriberReplicaId() <= 0 || open.WriterReplicaId == open.SubscriberReplicaId {
		return status.NewInvalidArgument("expected writer and reader identities in opening WAL replica watch")
	}
	route := types.PChannelInfo{Name: open.Pchannel, Term: open.WriterTerm, AccessMode: types.AccessModeRW, WALReplicaID: open.WriterReplicaId}
	local, err := s.manager.GetAvailableWAL(route)
	if err != nil {
		return err
	}
	provider, ok := wal.Unwrap(local).(interface{ ReplicaPublisher() *replica.Publisher })
	if !ok || provider.ReplicaPublisher() == nil {
		return status.NewChannelNotExist(open.Pchannel)
	}
	sub, snapshot, err := provider.ReplicaPublisher().Subscribe(open.SubscriberReplicaId)
	if err != nil {
		return err
	}
	defer sub.Close()
	if err := stream.Send(&streamingpb.WatchWALReplicaStateResponse{Response: &streamingpb.WatchWALReplicaStateResponse_Snapshot{Snapshot: snapshot}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)
	go func() {
		for {
			request, err := stream.Recv()
			if err != nil {
				cancel(err)
				return
			}
			report := request.GetViewReport()
			key := report.GetView()
			if key.GetShard() == nil || key.GetVersion().GetDataVersion() == nil {
				cancel(status.NewInvalidArgument("expected a versioned View report on WAL replica watch"))
				return
			}
			sub.Report(report)
		}
	}()
	for {
		changes, err := sub.Next(ctx)
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				err = cause
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.Send(&streamingpb.WatchWALReplicaStateResponse{Response: &streamingpb.WatchWALReplicaStateResponse_Changes{Changes: changes}}); err != nil {
			return err
		}
	}
}

func (s *WALReplicaService) PreparingWALHandoff(ctx context.Context, req *streamingpb.WALHandoffRequest) (*streamingpb.PreparingWALHandoffResponse, error) {
	tick, err := s.control.PreparingHandoff(ctx, req, contextutil.WALReplicaCaller(ctx))
	if err != nil {
		return nil, err
	}
	return &streamingpb.PreparingWALHandoffResponse{PersistedTimeTick: tick}, nil
}

func (s *WALReplicaService) WALHandoffCatchup(ctx context.Context, req *streamingpb.WALHandoffRequest) (*streamingpb.WALHandoffCatchupResponse, error) {
	if err := s.control.CatchupHandoff(ctx, req, contextutil.WALReplicaCaller(ctx)); err != nil {
		return nil, err
	}
	return &streamingpb.WALHandoffCatchupResponse{}, nil
}

func (s *WALReplicaService) SealWALHandoff(ctx context.Context, req *streamingpb.WALHandoffRequest) (*streamingpb.SealWALHandoffResponse, error) {
	if err := s.control.SealHandoff(ctx, req, contextutil.WALReplicaCaller(ctx)); err != nil {
		return nil, err
	}
	return &streamingpb.SealWALHandoffResponse{}, nil
}
