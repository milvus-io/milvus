package proxy

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// type pickShardPolicy func(ctx context.Context, mgr shardClientMgr, query func(UniqueID, types.QueryNode) error, leaders []nodeInfo) error

type queryFunc func(context.Context, UniqueID, types.QueryNode, ...string) error

type pickShardPolicy func(context.Context, shardClientMgr, queryFunc, map[string][]nodeInfo) error

var (
	errInvalidShardLeaders = errors.New("Invalid shard leader")
)

// RoundRobinPolicy do the query with multiple dml channels
// if request failed, it finds shard leader for failed dml channels
func RoundRobinPolicy(
	ctx context.Context,
	mgr shardClientMgr,
	query queryFunc,
	dml2leaders map[string][]nodeInfo) error {

	queryChannel := func(ctx context.Context, channel string) error {
		var combineErr error
		leaders := dml2leaders[channel]

		for _, target := range leaders {
			qn, err := mgr.GetClient(ctx, target.nodeID)
			if err != nil {
				log.Warn("query channel failed, node not available", zap.String("channel", channel), zap.Int64("nodeID", target.nodeID), zap.Error(err))
				combineErr = merr.Combine(combineErr, err)
				continue
			}
			err = query(ctx, target.nodeID, qn, channel)
			if err != nil {
				log.Warn("query channel failed", zap.String("channel", channel), zap.Int64("nodeID", target.nodeID), zap.Error(err))
				combineErr = merr.Combine(combineErr, err)
				continue
			}
			return nil
		}

		log.Ctx(ctx).Error("failed to do query on all shard leader",
			zap.String("channel", channel), zap.Error(combineErr))
		return combineErr
	}

	wg, ctx := errgroup.WithContext(ctx)
	for channel := range dml2leaders {
		channel := channel
		wg.Go(func() error {
			err := queryChannel(ctx, channel)
			return err
		})
	}

	err := wg.Wait()
	return err
}
