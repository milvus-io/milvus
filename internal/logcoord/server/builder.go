package server

import (
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/kv/logcoord"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	return &Server{
		session:               s.session,
		catalog:               logcoord.NewCataLog(s.metaKV),
		etcdClient:            s.etcdClient,
		componentStateService: componentutil.NewComponentStateService(typeutil.LogCoordRole),
	}
}
