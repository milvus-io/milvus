package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ServerBuilder is used to build a server.
// All component should be initialized before server initialization should be added here.
type ServerBuilder struct {
	etcdClient   *clientv3.Client
	grpcServer   *grpc.Server
	rc           types.RootCoordClient
	dc           types.DataCoordClient
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
func (b *ServerBuilder) WithRootCoordClient(rc types.RootCoordClient) *ServerBuilder {
	b.rc = rc
	return b
}

// WithDataCoordClient sets data coord client to the server builder.
func (b *ServerBuilder) WithDataCoordClient(dc types.DataCoordClient) *ServerBuilder {
	b.dc = dc
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
	resource.Apply(
		resource.OptETCD(b.etcdClient),
		resource.OptRootCoordClient(b.rc),
		resource.OptDataCoordClient(b.dc),
		resource.OptStreamingNodeCatalog(streamingnode.NewCataLog(b.kv)),
	)
	resource.Apply(
		resource.OptFlusher(flusherimpl.NewFlusher(b.chunkManager)),
	)
	resource.Done()
	return &Server{
		session:               b.session,
		grpcServer:            b.grpcServer,
		componentStateService: componentutil.NewComponentStateService(typeutil.StreamingNodeRole),
	}
}
