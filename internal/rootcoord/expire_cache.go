package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type expireCacheConfig struct {
	withDropFlag bool
}

func (c expireCacheConfig) apply(req *proxypb.InvalidateCollMetaCacheRequest) {
	if !c.withDropFlag {
		return
	}
	if req.GetBase() == nil {
		req.Base = &commonpb.MsgBase{}
	}
	req.Base.MsgType = commonpb.MsgType_DropCollection
}

func defaultExpireCacheConfig() expireCacheConfig {
	return expireCacheConfig{withDropFlag: false}
}

type expireCacheOpt func(c *expireCacheConfig)

func expireCacheWithDropFlag() expireCacheOpt {
	return func(c *expireCacheConfig) {
		c.withDropFlag = true
	}
}

// ExpireMetaCache will call invalidate collection meta cache
func (c *Core) ExpireMetaCache(ctx context.Context, collNames []string, collectionID UniqueID, ts typeutil.Timestamp, opts ...expireCacheOpt) error {
	// if collectionID is specified, invalidate all the collection meta cache with the specified collectionID and return
	if collectionID != InvalidCollectionID {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionID: collectionID,
		}
		return c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req, opts...)
	}

	// if only collNames are specified, invalidate the collection meta cache with the specified collectionName
	for _, collName := range collNames {
		req := proxypb.InvalidateCollMetaCacheRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0, //TODO, msg type
				MsgID:     0, //TODO, msg id
				Timestamp: ts,
				SourceID:  c.session.ServerID,
			},
			CollectionName: collName,
		}
		err := c.proxyClientManager.InvalidateCollectionMetaCache(ctx, &req, opts...)
		if err != nil {
			// TODO: try to expire all or directly return err?
			return err
		}
	}
	return nil
}
