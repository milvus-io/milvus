package proxy

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
)

func TestDatabaseInterceptor(t *testing.T) {
	ctx := context.Background()
	interceptor := DatabaseInterceptor()

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "", nil
	}

	t.Run("empty md", func(t *testing.T) {
		req := &milvuspb.CreateCollectionRequest{}
		_, err := interceptor(ctx, req, &grpc.UnaryServerInfo{}, handler)
		assert.NoError(t, err)
		assert.Equal(t, util.DefaultDBName, req.GetDbName())
	})

	t.Run("with invalid metadata", func(t *testing.T) {
		md := metadata.Pairs("xxx", "yyy")
		ctx = metadata.NewIncomingContext(ctx, md)
		req := &milvuspb.CreateCollectionRequest{}
		_, err := interceptor(ctx, req, &grpc.UnaryServerInfo{}, handler)
		assert.NoError(t, err)
		assert.Equal(t, util.DefaultDBName, req.GetDbName())
	})

	t.Run("empty req", func(t *testing.T) {
		md := metadata.Pairs("xxx", "yyy")
		ctx = metadata.NewIncomingContext(ctx, md)
		_, err := interceptor(ctx, "", &grpc.UnaryServerInfo{}, handler)
		assert.NoError(t, err)
	})

	t.Run("test ok for all request", func(t *testing.T) {
		availableReqs := []proto.Message{
			&milvuspb.CreateCollectionRequest{},
			&milvuspb.DropCollectionRequest{},
			&milvuspb.HasCollectionRequest{},
			&milvuspb.LoadCollectionRequest{},
			&milvuspb.ReleaseCollectionRequest{},
			&milvuspb.DescribeCollectionRequest{},
			&milvuspb.GetStatisticsRequest{},
			&milvuspb.GetCollectionStatisticsRequest{},
			&milvuspb.ShowCollectionsRequest{},
			&milvuspb.AlterCollectionRequest{},
			&milvuspb.CreatePartitionRequest{},
			&milvuspb.DropPartitionRequest{},
			&milvuspb.HasPartitionRequest{},
			&milvuspb.LoadPartitionsRequest{},
			&milvuspb.ReleasePartitionsRequest{},
			&milvuspb.GetPartitionStatisticsRequest{},
			&milvuspb.ShowPartitionsRequest{},
			&milvuspb.GetLoadingProgressRequest{},
			&milvuspb.GetLoadStateRequest{},
			&milvuspb.CreateIndexRequest{},
			&milvuspb.DescribeIndexRequest{},
			&milvuspb.DropIndexRequest{},
			&milvuspb.GetIndexBuildProgressRequest{},
			&milvuspb.GetIndexStateRequest{},
			&milvuspb.InsertRequest{},
			&milvuspb.DeleteRequest{},
			&milvuspb.SearchRequest{},
			&milvuspb.FlushRequest{},
			&milvuspb.QueryRequest{},
			&milvuspb.CreateAliasRequest{},
			&milvuspb.DropAliasRequest{},
			&milvuspb.AlterAliasRequest{},
			&milvuspb.GetPersistentSegmentInfoRequest{},
			&milvuspb.GetQuerySegmentInfoRequest{},
			&milvuspb.LoadBalanceRequest{},
			&milvuspb.GetReplicasRequest{},
			&milvuspb.ImportRequest{},
			&milvuspb.RenameCollectionRequest{},
			&milvuspb.TransferReplicaRequest{},
			&milvuspb.ListImportTasksRequest{},
			&milvuspb.OperatePrivilegeRequest{Entity: &milvuspb.GrantEntity{}},
			&milvuspb.SelectGrantRequest{Entity: &milvuspb.GrantEntity{}},
		}

		md := metadata.Pairs(util.HeaderDBName, "db")
		ctx = metadata.NewIncomingContext(ctx, md)
		for _, req := range availableReqs {
			before, err := proto.Marshal(req)
			assert.NoError(t, err)

			_, err = interceptor(ctx, req, &grpc.UnaryServerInfo{}, handler)
			assert.NoError(t, err)

			after, err := proto.Marshal(req)
			assert.NoError(t, err)

			assert.True(t, len(after) > len(before))
		}

		unavailableReqs := []proto.Message{
			&milvuspb.GetMetricsRequest{},
			&milvuspb.DummyRequest{},
			&milvuspb.CalcDistanceRequest{},
			&milvuspb.FlushAllRequest{},
			&milvuspb.GetCompactionStateRequest{},
			&milvuspb.ManualCompactionRequest{},
			&milvuspb.GetCompactionPlansRequest{},
			&milvuspb.GetFlushStateRequest{},
			&milvuspb.GetFlushAllStateRequest{},
			&milvuspb.GetImportStateRequest{},
		}

		for _, req := range unavailableReqs {
			before, err := proto.Marshal(req)
			assert.NoError(t, err)

			_, err = interceptor(ctx, req, &grpc.UnaryServerInfo{}, handler)
			assert.NoError(t, err)

			after, err := proto.Marshal(req)
			assert.NoError(t, err)

			if len(after) != len(before) {
				t.Errorf("req has been modified:%s", req.String())
			}
		}
	})

}
