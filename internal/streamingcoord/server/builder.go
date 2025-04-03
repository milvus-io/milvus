package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

type ServerBuilder struct {
	etcdClient     *clientv3.Client
	metaKV         kv.MetaKv
	session        sessionutil.SessionInterface
	mixCoordClient *syncutil.Future[types.MixCoordClient]
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

func (b *ServerBuilder) WithMixCoordClient(mixCoordClient *syncutil.Future[types.MixCoordClient]) *ServerBuilder {
	b.mixCoordClient = mixCoordClient
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
		resource.OptMixCoordClient(s.mixCoordClient),
	)
	balancer := syncutil.NewFuture[balancer.Balancer]()
	broadcaster := syncutil.NewFuture[broadcaster.Broadcaster]()
	return &Server{
		logger:            resource.Resource().Logger().With(log.FieldComponent("server")),
		session:           s.session,
		assignmentService: service.NewAssignmentService(balancer),
		broadcastService:  service.NewBroadcastService(broadcaster),
		balancer:          balancer,
		broadcaster:       broadcaster,
	}
}
