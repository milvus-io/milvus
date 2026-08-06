package rootcoord

import (
	"context"
	"net"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	tilib "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/catalogservice"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogrootcoord "github.com/milvus-io/milvus/internal/metastore/catalogservice/rootcoord"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestEtcdRootCoordCatalogCutoverToCatalogServiceAndNamespaceTransferE2E(t *testing.T) {
	ctx := context.Background()

	legacyEtcdKV := newE2EEmbeddedEtcdKV(t, "/milvus-e2e/legacy")
	legacyCatalog := kvrootcoord.NewCatalog(legacyEtcdKV)
	seedRootCoordCatalogE2E(t, ctx, legacyCatalog)

	tikvClient := newE2ETiKVClient(t)
	sourceBackend := kvrootcoord.NewCatalog(tikvkv.NewTiKV(tikvClient, "/milvus-e2e/catalog/milvus1"))
	targetBackend := kvrootcoord.NewCatalog(tikvkv.NewTiKV(tikvClient, "/milvus-e2e/catalog/milvus2"))

	rootCatalogClient, cleanup := newE2ERootCatalogClient(t, map[string]metastore.RootCoordCatalog{
		"milvus1": sourceBackend,
		"milvus2": targetBackend,
	})
	defer cleanup()

	sourceRemote := catalogrootcoord.NewCatalog(rootCatalogClient, "milvus1")
	targetRemote := catalogrootcoord.NewCatalog(rootCatalogClient, "milvus2")

	result, err := migrateRootCoordCatalogSnapshot(ctx, legacyCatalog, sourceRemote, 200)
	require.NoError(t, err)
	require.Equal(t, rootCoordCatalogMigrationResult{
		Databases:     1,
		Collections:   1,
		Aliases:       1,
		FileResources: 1,
	}, result)

	got, err := sourceRemote.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(1000), got.CollectionID)
	require.Len(t, got.Partitions, 1)

	aliases, err := sourceRemote.ListAliases(ctx, 10, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, aliases, 1)
	require.Equal(t, "alias1", aliases[0].Name)

	resources, version, err := sourceRemote.ListFileResource(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	require.Len(t, resources, 1)

	require.NoError(t, targetRemote.CreateDatabase(ctx, &model.Database{ID: 10, Name: "db1"}, 250))

	sourceRoot := &recordingRootCoordTransferE2E{}
	targetRoot := &recordingRootCoordTransferE2E{}
	manager := catalogservice.NewTransferManager(
		catalogservice.StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{
			"milvus1": sourceBackend,
			"milvus2": targetBackend,
		}),
		catalogservice.StaticTransferRootCoordResolver(map[string]catalogservice.TransferRootCoord{
			"milvus1": sourceRoot,
			"milvus2": targetRoot,
		}),
		catalogservice.NewMemoryTransferJobStore(),
	)

	transfer, err := manager.StartCollectionTransfer(ctx, catalogservice.StartCollectionTransferRequest{
		TransferID:      "transfer-e2e-1",
		TransferEpoch:   1,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db1",
		CollectionName:  "coll1",
		CommitTs:        300,
		CacheExpireTs:   301,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, catalogservice.TransferStateDone, transfer.State)
	require.Equal(t, int64(1000), transfer.CollectionID)

	require.NotNil(t, sourceRoot.prepare)
	require.Equal(t, int64(1000), sourceRoot.prepare.GetCollectionId())
	require.NotNil(t, sourceRoot.deactivate)
	require.Equal(t, []string{"alias1"}, sourceRoot.deactivate.GetAliases())
	require.Equal(t, uint64(301), sourceRoot.deactivate.GetCacheExpireTs())
	require.NotNil(t, targetRoot.apply)
	require.Equal(t, "coll1", targetRoot.apply.GetCollection().GetSchema().GetName())
	require.Len(t, targetRoot.apply.GetPartitions(), 1)
	require.Len(t, targetRoot.apply.GetAliases(), 1)

	require.False(t, sourceRemote.CollectionExists(ctx, 10, 1000, typeutil.MaxTimestamp))
	moved, err := targetRemote.GetCollectionByName(ctx, 10, "db1", "coll1", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Equal(t, int64(1000), moved.CollectionID)
	require.Len(t, moved.Partitions, 1)

	targetAliases, err := targetRemote.ListAliases(ctx, 10, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.Len(t, targetAliases, 1)
	require.Equal(t, "alias1", targetAliases[0].Name)
	require.Equal(t, int64(1000), targetAliases[0].CollectionID)
}

func seedRootCoordCatalogE2E(t *testing.T, ctx context.Context, catalog metastore.RootCoordCatalog) {
	t.Helper()

	require.NoError(t, catalog.CreateDatabase(ctx, &model.Database{ID: 10, Name: "db1"}, 100))
	require.NoError(t, catalog.CreateCollection(ctx, &model.Collection{
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
		State:                etcdpb.CollectionState_CollectionCreated,
		ShardsNum:            1,
		VirtualChannelNames:  []string{"by-dev-rootcoord-dml_0_1000v0"},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0"},
		ShardInfos: map[string]*model.ShardInfo{
			"by-dev-rootcoord-dml_0_1000v0": {
				VChannelName: "by-dev-rootcoord-dml_0_1000v0",
				PChannelName: "by-dev-rootcoord-dml_0",
			},
		},
	}, 101))
	require.NoError(t, catalog.CreateAlias(ctx, &model.Alias{
		Name:         "alias1",
		CollectionID: 1000,
		DbID:         10,
		State:        etcdpb.AliasState_AliasCreated,
	}, 102))
	require.NoError(t, catalog.SaveFileResource(ctx, &internalpb.FileResourceInfo{Id: 99, Name: "resource1"}, 1))
}

func newE2EEmbeddedEtcdKV(t *testing.T, rootPath string) *etcdkv.EmbedEtcdKV {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(t.TempDir(), "etcd")
	cfg.LogLevel = "warn"
	cfg.LogOutputs = []string{"default"}

	clientURL, err := url.Parse("http://localhost:0")
	require.NoError(t, err)
	peerURL, err := url.Parse("http://localhost:0")
	require.NoError(t, err)
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}

	kv, err := etcdkv.NewEmbededEtcdKV(cfg, rootPath)
	require.NoError(t, err)
	t.Cleanup(kv.Close)
	return kv
}

func newE2ETiKVClient(t *testing.T) *txnkv.Client {
	t.Helper()

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tilib.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return &txnkv.Client{KVStore: store}
}

func newE2ERootCatalogClient(t *testing.T, catalogs map[string]metastore.RootCoordCatalog) (catalogpb.RootCatalogServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	catalogpb.RegisterRootCatalogServiceServer(server, catalogservice.NewRootCatalogServer(
		catalogservice.StaticRootCoordCatalogResolver(catalogs),
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

type recordingRootCoordTransferE2E struct {
	prepare    *rootcoordpb.CatalogTransferPrepareRequest
	deactivate *rootcoordpb.CatalogTransferDeactivateRequest
	apply      *rootcoordpb.CatalogTransferApplyRequest
	abort      *rootcoordpb.CatalogTransferAbortRequest
}

func (r *recordingRootCoordTransferE2E) CatalogTransferPrepare(ctx context.Context, req *rootcoordpb.CatalogTransferPrepareRequest) error {
	r.prepare = req
	return nil
}

func (r *recordingRootCoordTransferE2E) CatalogTransferDeactivate(ctx context.Context, req *rootcoordpb.CatalogTransferDeactivateRequest) error {
	r.deactivate = req
	return nil
}

func (r *recordingRootCoordTransferE2E) CatalogTransferApply(ctx context.Context, req *rootcoordpb.CatalogTransferApplyRequest) error {
	r.apply = req
	return nil
}

func (r *recordingRootCoordTransferE2E) CatalogTransferAbort(ctx context.Context, req *rootcoordpb.CatalogTransferAbortRequest) error {
	r.abort = req
	return nil
}
