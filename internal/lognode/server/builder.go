package server

import (
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// ServerBuilder is used to build a server.
// All component should be initialized before server initialization should be added here.
type ServerBuilder struct {
	etcdClient *clientv3.Client
	grpcServer *grpc.Server
	rc         types.RootCoordClient
	session    *sessionutil.Session
}

func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{}
}

func (b *ServerBuilder) WithETCD(e *clientv3.Client) *ServerBuilder {
	b.etcdClient = e
	return b
}

func (b *ServerBuilder) WithGRPCServer(svr *grpc.Server) *ServerBuilder {
	b.grpcServer = svr
	return b
}

func (b *ServerBuilder) WithRootCoordClient(rc types.RootCoordClient) *ServerBuilder {
	b.rc = rc
	return b
}

func (b *ServerBuilder) WithSession(session *sessionutil.Session) *ServerBuilder {
	b.session = session
	return b
}

func (s *ServerBuilder) Build() *Server {
	return &Server{
		session:               s.session,
		rc:                    s.rc,
		etcdClient:            s.etcdClient,
		grpcServer:            s.grpcServer,
		componentStateService: componentutil.NewComponentStateService(typeutil.LogNodeRole),
	}
}
