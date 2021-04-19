package proxyservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Component = typeutil.Component
type Service = typeutil.Service

type ProxyService interface {
	Component
	Service
	RegisterLink() (*milvuspb.RegisterLinkResponse, error)
	RegisterNode(request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error)
	// TODO: i'm sure it's not a best way to keep consistency, fix me
	InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}
