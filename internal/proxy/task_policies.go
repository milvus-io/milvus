package proxy

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
)

type pickShardPolicy func(ctx context.Context, mgr *shardClientMgr, query func(UniqueID, types.QueryNode) error, leaders []nodeInfo) error

var (
	errBegin               = errors.New("begin error")
	errInvalidShardLeaders = errors.New("Invalid shard leader")
)

func updateShardsWithRoundRobin(shardsLeaders map[string][]nodeInfo) {
	for channelID, leaders := range shardsLeaders {
		if len(leaders) <= 1 {
			continue
		}

		shardsLeaders[channelID] = append(leaders[1:], leaders[0])
	}
}

func roundRobinPolicy(ctx context.Context, mgr *shardClientMgr, query func(UniqueID, types.QueryNode) error, leaders []nodeInfo) error {
	var (
		err     = errBegin
		current = 0
		qn      types.QueryNode
	)
	replicaNum := len(leaders)

	for err != nil && current < replicaNum {
		currentID := leaders[current].nodeID
		if err != errBegin {
			log.Ctx(ctx).Warn("retry with another QueryNode",
				zap.Int("retries numbers", current),
				zap.Int64("nodeID", currentID))
		}

		qn, err = mgr.GetClient(ctx, leaders[current].nodeID)
		if err != nil {
			log.Ctx(ctx).Warn("fail to get valid QueryNode", zap.Int64("nodeID", currentID),
				zap.Error(err))
			current++
			continue
		}

		err = query(currentID, qn)
		if err != nil {
			log.Ctx(ctx).Warn("fail to Query with shard leader",
				zap.Int64("nodeID", currentID),
				zap.Error(err))
		}
		current++
	}

	if current == replicaNum && err != nil {
		log.Ctx(ctx).Warn("no shard leaders available",
			zap.String("leaders", fmt.Sprintf("%v", leaders)), zap.Error(err))
		// needs to return the error from query
		return err
	}
	return nil
}
