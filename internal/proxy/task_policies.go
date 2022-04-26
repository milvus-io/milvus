package proxy

import (
	"context"
	"errors"

	qnClient "github.com/milvus-io/milvus/internal/distributed/querynode/client"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
)

type getQueryNodePolicy func(context.Context, string) (types.QueryNode, error)

type pickShardPolicy func(ctx context.Context, policy getQueryNodePolicy, query func(UniqueID, types.QueryNode) error, leaders *querypb.ShardLeadersList) error

// TODO add another policy to enbale the use of cache
// defaultGetQueryNodePolicy creates QueryNode client for every address everytime
func defaultGetQueryNodePolicy(ctx context.Context, address string) (types.QueryNode, error) {
	qn, err := qnClient.NewClient(ctx, address)
	if err != nil {
		return nil, err
	}

	if err := qn.Init(); err != nil {
		return nil, err
	}

	if err := qn.Start(); err != nil {
		return nil, err
	}
	return qn, nil
}

var (
	errBegin               = errors.New("begin error")
	errInvalidShardLeaders = errors.New("Invalid shard leader")
)

func roundRobinPolicy(ctx context.Context, getQueryNodePolicy getQueryNodePolicy, query func(UniqueID, types.QueryNode) error, leaders *querypb.ShardLeadersList) error {
	var (
		err     = errBegin
		current = 0
		qn      types.QueryNode
	)
	replicaNum := len(leaders.GetNodeIds())

	for err != nil && current < replicaNum {
		currentID := leaders.GetNodeIds()[current]
		if err != errBegin {
			log.Warn("retry with another QueryNode", zap.String("leader", leaders.GetChannelName()), zap.Int64("nodeID", currentID))
		}

		qn, err = getQueryNodePolicy(ctx, leaders.GetNodeAddrs()[current])
		if err != nil {
			log.Warn("fail to get valid QueryNode", zap.Int64("nodeID", currentID),
				zap.Error(err))
			current++
			continue
		}

		defer qn.Stop()
		err = query(currentID, qn)
		if err != nil {
			log.Warn("fail to Query with shard leader",
				zap.String("leader", leaders.GetChannelName()),
				zap.Int64("nodeID", currentID),
				zap.Error(err))
		}
		current++
	}

	if current == replicaNum && err != nil {
		log.Warn("no shard leaders available for channel",
			zap.String("channel name", leaders.GetChannelName()),
			zap.Int64s("leaders", leaders.GetNodeIds()), zap.Error(err))
		// needs to return the error from query
		return err
	}
	return nil
}
