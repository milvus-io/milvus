package grpcquerynode

import (
	"google.golang.org/grpc"

	qnviewquery "github.com/milvus-io/milvus/internal/querynodev2/viewquery"
	sharedviewquery "github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func registerQueryViewQueryServer(grpcServer *grpc.Server, queryViewHandler sharedviewquery.TaskProvider, serverID int64) {
	executor := qnviewquery.NewDirectSegmentTaskExecutor(serverID)
	scheduler := qnviewquery.NewScheduler(executor)
	viewpb.RegisterViewQueryServiceServer(grpcServer, sharedviewquery.NewServer(queryViewHandler, scheduler))
}
