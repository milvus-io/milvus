package queryclient

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type legacyClient struct {
	shardClient   *shardViewQueryClient
	shardResolver resolver.ShardResolver
}

func (c *legacyClient) Search(ctx context.Context, request *LegacySearchRequest) (*LegacySearchResult, error) {
	if request == nil || request.Req == nil {
		return nil, merr.WrapErrParameterInvalid("non-nil legacy search request", "nil")
	}
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, request.Req.GetCollectionID())
	if err != nil {
		return nil, normalizeBoundaryError(err, "resolve query view vchannels")
	}
	if len(vchannels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg(
			"query view has no resolved vchannels for collection %d", request.Req.GetCollectionID())
	}
	collector := newLegacySearchCollector()
	plans := make([]ShardPlan, len(vchannels))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, vchannel := range vchannels {
		index, vchannel := index, vchannel
		group.Go(func() error {
			plan, shardErr := c.shardClient.Search(groupCtx, vchannel, request.Req, collector)
			if shardErr != nil {
				return shardErr
			}
			plans[index] = *plan
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, normalizeBoundaryError(err, "execute legacy search on query view")
	}
	return &LegacySearchResult{Results: collector.Results(), Plans: plans}, nil
}

func (c *legacyClient) Query(ctx context.Context, request *LegacyQueryRequest) (*LegacyQueryResult, error) {
	if request == nil || request.Req == nil {
		return nil, merr.WrapErrParameterInvalid("non-nil legacy query request", "nil")
	}
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, request.Req.GetCollectionID())
	if err != nil {
		return nil, normalizeBoundaryError(err, "resolve query view vchannels")
	}
	if len(vchannels) == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg(
			"query view has no resolved vchannels for collection %d", request.Req.GetCollectionID())
	}
	collector := newLegacyQueryCollector()
	plans := make([]ShardPlan, len(vchannels))
	group, groupCtx := errgroup.WithContext(ctx)
	for index, vchannel := range vchannels {
		index, vchannel := index, vchannel
		group.Go(func() error {
			plan, shardErr := c.shardClient.Query(groupCtx, vchannel, request.Req, collector)
			if shardErr != nil {
				return shardErr
			}
			plans[index] = *plan
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, normalizeBoundaryError(err, "execute legacy query on query view")
	}
	return &LegacyQueryResult{Results: collector.Results(), Plans: plans}, nil
}
