package server

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	catalogclient "github.com/milvus-io/milvus-catalog/client"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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
	var catalog metastore.StreamingCoordCataLog = streamingcoord.NewCataLog(s.metaKV)
	if paramtable.Get().MetaStoreCfg.UseCatalogService.GetAsBool() {
		rootPath := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()
		if paramtable.Get().MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
			rootPath = paramtable.Get().TiKVCfg.MetaRootPath.GetValue()
		}
		remote, err := catalogclient.NewCatalogServiceClient(paramtable.Get().MetaStoreCfg.CatalogServiceAddr.GetValue(), rootPath)
		if err != nil {
			log.Fatal("failed to connect catalog service", zap.Error(err))
		}
		catalog = remote
	}
	resource.Init(
		resource.OptETCD(s.etcdClient),
		resource.OptStreamingCatalog(catalog),
		resource.OptMixCoordClient(s.mixCoordClient),
		resource.OptSession(s.session),
	)
	return &Server{
		logger:            resource.Resource().Logger().With(log.FieldComponent("server")),
		session:           s.session,
		assignmentService: service.NewAssignmentService(),
		broadcastService:  service.NewBroadcastService(),
	}
}
