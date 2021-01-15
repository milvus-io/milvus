package proxyservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type ServiceBase = typeutil.Service

type Interface interface {
	ServiceBase
	RegisterLink() (proxypb.RegisterLinkResponse, error)
	RegisterNode(request proxypb.RegisterNodeRequest) (proxypb.RegisterNodeResponse, error)
	// TODO: i'm sure it's not a best way to keep consistency, fix me
	InvalidateCollectionMetaCache(request proxypb.InvalidateCollMetaCacheRequest) error
}
