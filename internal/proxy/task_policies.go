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
			log.Warn("retry with another QueryNode",
				zap.Int("retries numbers", current),
				zap.Int64("nodeID", currentID))
		}

		qn, err = mgr.GetClient(ctx, leaders[current].nodeID)
		if err != nil {
			log.Warn("fail to get valid QueryNode", zap.Int64("nodeID", currentID),
				zap.Error(err))
			current++
			continue
		}

		err = query(currentID, qn)
		if err != nil {
			log.Warn("fail to Query with shard leader",
				zap.Int64("nodeID", currentID),
				zap.Error(err))
		}
		current++
	}

	if current == replicaNum && err != nil {
		log.Warn("no shard leaders available",
			zap.String("leaders", fmt.Sprintf("%v", leaders)), zap.Error(err))
		// needs to return the error from query
		return err
	}
	return nil
}

// use policy to select shard leader, then group shard leaders within same query node
func groupShardLeadersWithSameQueryNode(
	ctx context.Context,
	policy pickShardPolicy,
	mgr *shardClientMgr,
	shard2leaders map[string][]nodeInfo) (map[int64][]string, map[int64]types.QueryNode, error) {
	node2dmlChans := make(map[int64][]string)
	node2QNode := make(map[int64]types.QueryNode)

	for dmlChan, leaders := range shard2leaders {
		ch := dmlChan
		leaders := leaders
		if err := policy(ctx, mgr, func(nodeId int64, qn types.QueryNode) error {
			node2QNode[nodeId] = qn
			if _, ok := node2dmlChans[nodeId]; !ok {
				node2dmlChans[nodeId] = make([]string, 0)
			}
			node2dmlChans[nodeId] = append(node2dmlChans[nodeId], ch)
			return nil
		}, leaders); err != nil {
			return nil, nil, err
		}
	}

	log.Debug("group shard leaders with same query node", zap.Any("node2dmlChannels", node2dmlChans))

	return node2dmlChans, node2QNode, nil
}
