package server

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	pkindexclient "github.com/milvus-io/milvus/internal/datacoord/pkindex/client"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/indexmanager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// ServerBuilder is used to build a server.
// All component should be initialized before server initialization should be added here.
type ServerBuilder struct {
	etcdClient   *clientv3.Client
	grpcServer   *grpc.Server
	mixc         *syncutil.Future[types.MixCoordClient]
	session      *sessionutil.Session
	kv           kv.MetaKv
	chunkManager storage.ChunkManager
}

// NewServerBuilder creates a new server builder.
func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{}
}

// WithETCD sets etcd client to the server builder.
func (b *ServerBuilder) WithETCD(e *clientv3.Client) *ServerBuilder {
	b.etcdClient = e
	return b
}

// WithChunkManager sets chunk manager to the server builder.
func (b *ServerBuilder) WithChunkManager(cm storage.ChunkManager) *ServerBuilder {
	b.chunkManager = cm
	return b
}

// WithGRPCServer sets grpc server to the server builder.
func (b *ServerBuilder) WithGRPCServer(svr *grpc.Server) *ServerBuilder {
	b.grpcServer = svr
	return b
}

// WithRootCoordClient sets root coord client to the server builder.
func (b *ServerBuilder) WithMixCoordClient(mixc *syncutil.Future[types.MixCoordClient]) *ServerBuilder {
	b.mixc = mixc
	return b
}

// WithSession sets session to the server builder.
func (b *ServerBuilder) WithSession(session *sessionutil.Session) *ServerBuilder {
	b.session = session
	return b
}

// WithMetaKV sets meta kv to the server builder.
func (b *ServerBuilder) WithMetaKV(kv kv.MetaKv) *ServerBuilder {
	b.kv = kv
	return b
}

// Build builds a streaming node server.
func (b *ServerBuilder) Build() *Server {
	// The index manager uses the same chunk manager as the resource. It reads the ID allocator from the resource only when a WAL
	// opens, which is after resource.Init. The manifest client needs only the etcd client, not the resource.
	mgr, closePKIndex, err := indexmanager.Setup(context.TODO(), b.chunkManager, func(ctx context.Context) (manifest.Committer, func(), error) {
		cli := pkindexclient.NewClient(b.etcdClient)
		return cli, cli.Close, nil
	}, func(ctx context.Context) (int64, error) {
		id, err := resource.Resource().IDAllocator().Allocate(ctx)
		return int64(id), err
	})
	if err != nil {
		panic(fmt.Sprintf("setup the primary key index manager failed: %v", err))
	}
	resource.Init(
		resource.OptETCD(b.etcdClient),
		resource.OptChunkManager(b.chunkManager),
		resource.OptPKIndexManager(mgr),
		resource.OptMixCoordClient(b.mixc),
		resource.OptStreamingNodeCatalog(streamingnode.NewCataLog(b.kv)),
	)
	s := &Server{
		session:      b.session,
		grpcServer:   b.grpcServer,
		pkindex:      mgr,
		closePKIndex: closePKIndex,
	}
	s.init()
	return s
}
