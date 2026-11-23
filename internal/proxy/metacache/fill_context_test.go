package metacache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMetaCacheFillDeadline(t *testing.T) {
	param := &paramtable.Get().ProxyCfg.MetaCacheFillTimeout
	for _, value := range []string{"0", "-1", "invalid"} {
		t.Run(value, func(t *testing.T) {
			paramtable.Get().Save(param.Key, value)
			defer paramtable.Get().Reset(param.Key)
			require.Equal(t, param.DefaultValue, param.GetValue())
		})
	}

	ctx, cancel := context.WithTimeoutCause(t.Context(), 0, errFillTimeout)
	defer cancel()
	for _, err := range []error{context.DeadlineExceeded, status.Error(codes.DeadlineExceeded, "coordinator call timed out")} {
		failure := fillError(ctx, err)
		require.ErrorIs(t, failure, err)
		wire := merr.Status(failure)
		require.Equal(t, merr.Code(merr.ErrServiceUnavailable), wire.GetCode())
		require.True(t, wire.GetRetriable())
	}

	notFound := merr.WrapErrCollectionNotFound("c")
	require.Equal(t, notFound, fillError(ctx, notFound))
	parent, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	fill, stopFill := newFillContext(parent)
	defer stopFill()
	require.Equal(t, context.DeadlineExceeded, fillError(fill, fill.Err()))
}

func TestMetaCacheFillTimeoutReleasesBlockedInvalidation(t *testing.T) {
	for _, kind := range []string{"collection", "partition", "database"} {
		t.Run(kind, func(t *testing.T) {
			key := paramtable.Get().ProxyCfg.MetaCacheFillTimeout.Key
			paramtable.Get().Save(key, "0.1")
			defer paramtable.Get().Reset(key)

			rpcStarted := make(chan struct{})
			rpcDone := make(chan struct{})
			abort := make(chan struct{})
			defer close(abort)
			park := func(ctx context.Context) error {
				close(rpcStarted)
				defer close(rpcDone)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-abort:
					return context.Canceled
				}
			}
			root := mocks.NewMockMixCoordClient(t)
			cache, err := NewMetaCache(root)
			require.NoError(t, err)
			defer cache.Close()
			ctx := context.Background()
			fillDone := make(chan error, 1)
			switch kind {
			case "collection":
				root.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
						return nil, park(ctx)
					})
				go func() {
					_, err := cache.GetCollectionID(ctx, "db", "c")
					fillDone <- err
				}()
			case "partition":
				schema, err := NewSchemaInfo(&schemapb.CollectionSchema{
					Name: "c", Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
				})
				require.NoError(t, err)
				cache.collections[1] = &CollectionInfo{CollID: 1, DBName: "db", Schema: schema}
				cache.nameIdx["db"] = map[string]int64{"c": 1}
				root.EXPECT().ShowPartitions(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ *milvuspb.ShowPartitionsRequest, _ ...grpc.CallOption) (*milvuspb.ShowPartitionsResponse, error) {
						return nil, park(ctx)
					})
				go func() {
					_, err := cache.GetPartitionInfos(ctx, "db", "c")
					fillDone <- err
				}()
			case "database":
				root.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ *rootcoordpb.DescribeDatabaseRequest, _ ...grpc.CallOption) (*rootcoordpb.DescribeDatabaseResponse, error) {
						return nil, park(ctx)
					})
				go func() {
					_, err := cache.GetDatabaseInfo(ctx, "db")
					fillDone <- err
				}()
			}

			select {
			case <-rpcStarted:
			case err := <-fillDone:
				t.Fatalf("fill returned before reaching %s RPC: %v", kind, err)
			case <-time.After(5 * time.Second):
				t.Fatal("fill did not reach RPC")
			}
			invalidated := make(chan struct{})
			go func() {
				cache.RemoveDatabaseInfo(ctx, "db")
				select {
				case <-rpcDone:
				default:
					t.Error("invalidation bypassed the active fill")
				}
				close(invalidated)
			}()
			select {
			case <-invalidated:
			case <-time.After(5 * time.Second):
				t.Fatal("invalidation stayed blocked behind the fill")
			}
			require.ErrorIs(t, <-fillDone, merr.ErrServiceUnavailable)
		})
	}
}

func TestMetaCacheSchemaFillJoinerSharesDeadline(t *testing.T) {
	// Initialize the parameter table outside the bubble, before it starts its file watcher.
	param := &paramtable.Get().ProxyCfg.MetaCacheFillTimeout
	paramtable.Get().Save(param.Key, "60")
	defer paramtable.Get().Reset(param.Key)
	synctest.Test(t, func(t *testing.T) {
		root := mocks.NewMockMixCoordClient(t)
		root.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(), DbName: "db", CollectionID: 1, Schema: &schemapb.CollectionSchema{Name: "c"},
			VirtualChannelNames: []string{"p_1v0"},
		}, nil).Once()
		client, wal := &schemaTestClient{}, &schemaTestWAL{}
		defer mockey.Mock(streaming.WAL).Return(wal).Build().UnPatch()
		defer mockey.Mock((*schemaTestWAL).SchemaClient).Return(client).Build().UnPatch()
		defer mockey.Mock((*schemaTestClient).GetSchemaVersion).To(func(_ *schemaTestClient, ctx context.Context, _ streaming.GetSchemaVersionRequest) (*SchemaInfo, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build().UnPatch()
		cache, err := NewMetaCache(root)
		require.NoError(t, err)
		defer cache.Close()
		results := make(chan error, 2)
		lookup := func() {
			_, err := cache.UpdateByName(context.Background(), "db", "c")
			results <- err
		}
		started := time.Now()
		go lookup()
		synctest.Wait()
		time.Sleep(30 * time.Second)
		go lookup()
		synctest.Wait()
		for range 2 {
			require.ErrorIs(t, <-results, merr.ErrServiceUnavailable)
			require.Equal(t, time.Minute, time.Since(started))
		}
		require.Empty(t, cache.collections)
	})
}
