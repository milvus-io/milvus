package testcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func createMultiSearchCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, name string, pk int64) {
	t.Helper()
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("marker").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(name)))
	})

	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name,
		column.NewColumnInt64("id", []int64{pk, pk + 1}),
		column.NewColumnInt64("marker", []int64{1000 + pk, 2000 + pk}),
		column.NewColumnFloatVector("vector", 2, [][]float32{{1, 0}, {0, 1}}),
	))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	flat, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.IP)))
	require.NoError(t, err)
	require.NoError(t, flat.Await(ctx))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
}

func TestMultiSearchOrderedResults(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	db := common.GenRandomString("multisearch_db", 6)
	require.NoError(t, mc.CreateDatabase(ctx, client.NewCreateDatabaseOption(db)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropDatabase(cleanupCtx, client.NewDropDatabaseOption(db)))
	})
	archive := hp.CreateMilvusClient(ctx, t, &client.ClientConfig{DBName: db})
	name := common.GenRandomString("multisearch", 6)
	createMultiSearchCollection(ctx, t, mc, name, 1)
	createMultiSearchCollection(ctx, t, archive, name, 101)

	vectors := []entity.Vector{entity.FloatVector{1, 0}, entity.FloatVector{0, 1}}
	req, err := client.NewSearchOption(name, 3, vectors).
		WithANNSField("vector").
		WithOutputFields("marker").
		WithConsistencyLevel(entity.ClStrong).
		Request()
	require.NoError(t, err)

	explicit := proto.CloneOf(req)
	explicit.DbName = "default"
	empty := proto.CloneOf(req)
	empty.Dsl = "id < 0"
	missing := proto.CloneOf(req)
	missing.CollectionName += "_missing"

	wrap := func(search *milvuspb.SearchRequest) *milvuspb.MultiSearchSubRequest {
		return &milvuspb.MultiSearchSubRequest{Request: &milvuspb.MultiSearchSubRequest_Search{Search: search}}
	}
	service := archive.GetService()
	batch := &milvuspb.MultiSearchRequest{
		Requests:       []*milvuspb.MultiSearchSubRequest{wrap(req), wrap(missing), wrap(explicit), wrap(empty)},
		MaxConcurrency: 2,
	}
	response, err := service.MultiSearch(ctx, batch)
	require.NoError(t, merr.CheckRPCCall(response, err))
	require.Len(t, response.GetResults(), 4)

	for _, i := range []int{0, 2, 3} {
		ordinary, err := service.Search(ctx, batch.Requests[i].GetSearch())
		require.NoError(t, merr.CheckRPCCall(ordinary, err))
		item := response.Results[i].GetSearch()
		require.NotNil(t, item)
		require.NoError(t, merr.Error(item.GetStatus()))
		require.Equal(t, ordinary.GetCollectionName(), item.GetCollectionName())
		require.True(t, proto.Equal(ordinary.GetResults(), item.GetResults()), "item %d differs from Search", i)
		require.EqualValues(t, 2, item.GetResults().GetNumQueries())
	}
	require.Equal(t, []int64{2, 2}, response.Results[0].GetSearch().GetResults().GetTopks())
	require.Equal(t, int64(101), response.Results[0].GetSearch().GetResults().GetIds().GetIntId().GetData()[0])
	require.Equal(t, int64(1), response.Results[2].GetSearch().GetResults().GetIds().GetIntId().GetData()[0])
	failure := response.Results[1].GetSearch()
	require.NotNil(t, failure)
	require.ErrorIs(t, merr.Error(failure.GetStatus()), merr.ErrCollectionNotFound)
	require.Nil(t, failure.GetResults())
	require.Empty(t, response.Results[3].GetSearch().GetResults().GetIds().GetIntId().GetData())

	t.Run("unused database metadata", func(t *testing.T) {
		missingDB := metadata.AppendToOutgoingContext(ctx, "dbname", db+"_missing")
		resp, err := service.MultiSearch(missingDB, &milvuspb.MultiSearchRequest{
			Requests: []*milvuspb.MultiSearchSubRequest{wrap(explicit)},
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), 1)
		require.NoError(t, merr.Error(resp.Results[0].GetSearch().GetStatus()))
	})
	t.Run("invalid operation", func(t *testing.T) {
		resp, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
			Requests: []*milvuspb.MultiSearchSubRequest{wrap(req), {}},
		})
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
		require.Empty(t, resp.GetResults())
	})
	t.Run("budget exhausted", func(t *testing.T) {
		zero := int64(0)
		resp, err := service.MultiSearch(ctx, &milvuspb.MultiSearchRequest{
			Requests:       batch.GetRequests(),
			TimeoutMs:      &zero,
			MaxConcurrency: 1,
		})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Len(t, resp.GetResults(), len(batch.GetRequests()))
		for _, item := range resp.GetResults() {
			require.NotNil(t, item.GetSearch())
			require.Equal(t, merr.TimeoutCode, item.GetSearch().GetStatus().GetCode())
			require.Nil(t, item.GetSearch().GetResults())
		}
	})
}
