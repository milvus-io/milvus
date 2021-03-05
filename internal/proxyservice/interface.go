package proxyservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Service interface {
	typeutil.Component
	typeutil.TimeTickHandler

	RegisterLink(ctx context.Context) (*milvuspb.RegisterLinkResponse, error)
	RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error)
	// TODO: i'm sure it's not a best way to keep consistency, fix me
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}
