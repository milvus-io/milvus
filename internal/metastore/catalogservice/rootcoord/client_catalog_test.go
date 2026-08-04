package rootcoord_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/catalogservice"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogrootcoord "github.com/milvus-io/milvus/internal/metastore/catalogservice/rootcoord"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestServiceBackedRootCoordCatalogDatabaseAndCollection(t *testing.T) {
	ctx := context.Background()
	localCatalog := kvrootcoord.NewCatalog(memkv.NewMemoryKV())
	client, cleanup := newBufconnRootCatalogClient(t, localCatalog)
	defer cleanup()

	remoteCatalog := catalogrootcoord.NewCatalog(client, "ns1")
	require.Implements(t, (*metastore.RootCoordCatalog)(nil), remoteCatalog)

	db := &model.Database{ID: 10, Name: "db1"}
	require.NoError(t, remoteCatalog.CreateDatabase(ctx, db, 100))

	dbs, err := remoteCatalog.ListDatabases(ctx, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Equal(t, "db1", dbs[0].Name)

	coll := &model.Collection{
		CollectionID: 1000,
		DBID:         10,
		DBName:       "db1",
		Name:         "coll1",
		Fields: []*model.Field{{
			FieldID:  100,
			Name:     "pk",
			DataType: schemapb.DataType_Int64,
		}},
		Partitions: []*model.Partition{{
			PartitionID:   2000,
			PartitionName: "_default",
			CollectionID:  1000,
			State:         etcdpb.PartitionState_PartitionCreated,
		}},
		State: etcdpb.CollectionState_CollectionCreated,
	}
	require.NoError(t, remoteCatalog.CreateCollection(ctx, coll, 101))

	got, err := remoteCatalog.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)
	require.Len(t, got.Partitions, 1)

	require.NoError(t, remoteCatalog.Update(ctx, 102, metastore.DropCollection(got.Clone())))
	require.False(t, remoteCatalog.CollectionExists(ctx, 10, 1000, typeutil.MaxTimestamp))
}

func newBufconnRootCatalogClient(t *testing.T, catalog metastore.RootCoordCatalog) (catalogpb.RootCatalogServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	catalogpb.RegisterRootCatalogServiceServer(server, catalogservice.NewRootCatalogServer(
		catalogservice.StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"ns1": catalog}),
	))
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)
	return catalogpb.NewRootCatalogServiceClient(conn), func() {
		conn.Close()
		server.Stop()
		listener.Close()
	}
}
