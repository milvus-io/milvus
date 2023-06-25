package proxy

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// DatabaseInterceptor fill dbname into request based on kv pair <"dbname": "xx"> in header
func DatabaseInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		filledCtx, filledReq := fillDatabase(ctx, req)
		return handler(filledCtx, filledReq)
	}
}

func fillDatabase(ctx context.Context, req interface{}) (context.Context, interface{}) {
	switch r := req.(type) {
	case *milvuspb.CreateCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DropCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.HasCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.LoadCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ReleaseCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DescribeCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetStatisticsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetCollectionStatisticsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ShowCollectionsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.AlterCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.CreatePartitionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DropPartitionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.HasPartitionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.LoadPartitionsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ReleasePartitionsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetPartitionStatisticsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ShowPartitionsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetLoadingProgressRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetLoadStateRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.CreateIndexRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DescribeIndexRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DropIndexRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetIndexBuildProgressRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetIndexStateRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.InsertRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DeleteRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.SearchRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.FlushRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.QueryRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.CreateAliasRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.DropAliasRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.AlterAliasRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ImportRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.ListImportTasksRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.RenameCollectionRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.TransferReplicaRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetPersistentSegmentInfoRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetQuerySegmentInfoRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.LoadBalanceRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetReplicasRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.OperatePrivilegeRequest:
		if r.Entity != nil && r.Entity.DbName == "" {
			r.Entity.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.SelectGrantRequest:
		if r.Entity != nil && r.Entity.DbName == "" {
			r.Entity.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.GetIndexStatisticsRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	case *milvuspb.UpsertRequest:
		if r.DbName == "" {
			r.DbName = GetCurDBNameFromContextOrDefault(ctx)
		}
		return ctx, r
	default:
		return ctx, req
	}
}
