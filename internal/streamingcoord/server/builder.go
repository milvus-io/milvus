package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ServerBuilder struct {
	etcdClient *clientv3.Client
	metaKV     kv.MetaKv
	session    sessionutil.SessionInterface
}

func NewServerBuilder() *ServerBuilder {
	return &ServerBuilder{}
}

func (b *ServerBuilder) WithETCD(e *clientv3.Client) *ServerBuilder {
	b.etcdClient = e
	return b
}

func (b *ServerBuilder) WithMetaKV(metaKV kv.MetaKv) *ServerBuilder {
	b.metaKV = metaKV
	return b
}

func (b *ServerBuilder) WithSession(session sessionutil.SessionInterface) *ServerBuilder {
	b.session = session
	return b
}

func (s *ServerBuilder) Build() *Server {
	resource.Init(
		resource.OptETCD(s.etcdClient),
		resource.OptStreamingCatalog(streamingcoord.NewCataLog(s.metaKV)),
	)
	balancer := syncutil.NewFuture[balancer.Balancer]()
	return &Server{
		session:               s.session,
		componentStateService: componentutil.NewComponentStateService(typeutil.StreamingCoordRole),
		assignmentService:     service.NewAssignmentService(balancer),
		balancer:              balancer,
	}
}
