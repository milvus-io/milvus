package snview

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ viewpb.ViewSyncServiceServer = (*PChannelViewSyncServer)(nil)

type PChannelViewSyncServer struct {
	viewpb.UnimplementedViewSyncServiceServer

	walManager pchannelWALProvider
}

func NewPChannelViewSyncServer(walManager pchannelWALProvider) *PChannelViewSyncServer {
	return &PChannelViewSyncServer{
		walManager: walManager,
	}
}

func (s *PChannelViewSyncServer) SyncQueryView(stream viewpb.ViewSyncService_SyncQueryViewServer) error {
	pchannel, err := handler.DecodeQueryViewPChannelFromIncomingContext(stream.Context())
	if err != nil {
		return asViewSyncStreamingGRPCError(err)
	}
	rawWAL, err := s.walManager.GetAvailableWAL(pchannel)
	if err != nil {
		return asViewSyncStreamingGRPCError(err)
	}
	provider, ok := wal.Unwrap(rawWAL).(QueryViewHandlerProvider)
	if !ok {
		return asViewSyncStreamingGRPCError(streamingstatus.NewChannelNotExist(pchannel.Name))
	}
	queryViewHandler := provider.QueryViewHandler()
	return handler.NewViewSyncServer(&pchannelScopedQueryViewHandler{
		pchannel: pchannel.Name,
		handler:  queryViewHandler,
	}).SyncQueryViewUntil(stream, rawWAL.Available())
}

func asViewSyncStreamingGRPCError(err error) error {
	if err == nil {
		return nil
	}
	return streamingstatus.NewGRPCStatusFromStreamingError(streamingstatus.AsStreamingError(err)).Err()
}

type pchannelScopedQueryViewHandler struct {
	pchannel string
	handler  handler.QueryViewHandler
}

func (h *pchannelScopedQueryViewHandler) ApplyViews(views []handler.ApplyView) {
	var matched []handler.ApplyView
	for _, view := range views {
		shardID := view.View.ShardID()
		if shardID == (qviews.ShardID{}) || funcutil.ToPhysicalChannel(shardID.VChannel) != h.pchannel {
			reportUnrecoverable([]handler.ApplyView{view})
			continue
		}
		matched = append(matched, view)
	}
	if len(matched) == 0 {
		return
	}
	if h.handler == nil {
		reportUnrecoverable(matched)
		return
	}
	h.handler.ApplyViews(matched)
}
