package grpcquerynode

import (
	"google.golang.org/grpc"

	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func registerQueryViewServers(grpcServer *grpc.Server, segMgr qnview.SegmentManager, serverID int64) {
	queryViewHandler := qnview.NewQNQueryViewHandler(segMgr)
	registerQueryViewSyncHandler(grpcServer, queryViewHandler)
	registerQueryViewQueryServer(grpcServer, queryViewHandler, serverID)
}

func (s *Server) registerQueryViewServers() {
	registerQueryViewServers(s.grpcServer, &lazyQNSegmentManager{
		build: func() qnview.SegmentManager {
			qnImpl, ok := s.querynode.(*qn.QueryNode)
			if !ok {
				return nil
			}
			var watcherFactory qnview.SegmentLoadInfoWatcherFactory
			mixCoord, err := s.mixCoord.GetWithContext(s.ctx)
			if err == nil {
				watcherFactory, _ = mixCoord.(qnview.SegmentLoadInfoWatcherFactory)
			}
			return qnImpl.NewQueryViewSegmentManager(
				s.ctx,
				&lazyQueryViewLoadMetadataProvider{mixCoord: s.mixCoord},
				queryViewTransformLogStreamManager(),
				watcherFactory,
			)
		},
	}, paramtable.GetNodeID())
}
