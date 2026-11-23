package grpcquerynode

import (
	"google.golang.org/grpc"

	qn "github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	qnviewquery "github.com/milvus-io/milvus/internal/querynodev2/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func registerQueryViewServers(grpcServer *grpc.Server, segMgr qnview.SegmentManager, executor qnviewquery.SegmentTaskExecutor) *qnview.QNQueryViewHandler {
	queryViewHandler := qnview.NewQNQueryViewHandler(segMgr)
	registerQueryViewSyncHandler(grpcServer, queryViewHandler)
	registerQueryViewQueryServer(grpcServer, queryViewHandler, executor)
	return queryViewHandler
}

func (s *Server) registerQueryViewServers() {
	node, _ := s.querynode.(*qn.QueryNode)
	executor := qnviewquery.NewDirectSegmentTaskExecutor(paramtable.GetNodeID(), node.ScheduleQueryViewRead)
	queryViewHandler := registerQueryViewServers(s.grpcServer, &lazyQNSegmentManager{
		scheduler: nodescheduler.Get(),
		build: func() qnview.SegmentManager {
			if node == nil {
				return nil
			}
			metadata := &lazyQueryViewLoadMetadataProvider{ctx: s.ctx, mixCoord: s.mixCoord}
			return node.NewQueryViewSegmentManager(
				s.ctx,
				metadata,
				queryViewTransformLogStreamManager(),
				metadata,
			)
		},
	}, executor)
	if node != nil {
		node.SetQueryViewHandler(queryViewHandler)
	}
}
