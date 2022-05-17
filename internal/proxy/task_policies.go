package proxy

import (
	"context"
	"errors"
	"fmt"

	qnClient "github.com/milvus-io/milvus/internal/distributed/querynode/client"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
)

type getQueryNodePolicy func(context.Context, string) (types.QueryNode, error)

type pickShardPolicy func(ctx context.Context, policy getQueryNodePolicy, query func(UniqueID, types.QueryNode) error, leaders []queryNode) error

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

type queryNode struct {
	nodeID  UniqueID
	address string
}

func (q queryNode) String() string {
	return fmt.Sprintf("<NodeID: %d>", q.nodeID)
}

func updateShardsWithRoundRobin(shardsLeaders map[string][]queryNode) map[string][]queryNode {

	for channelID, leaders := range shardsLeaders {
		if len(leaders) <= 1 {
			continue
		}

		shardsLeaders[channelID] = append(leaders[1:], leaders[0])
	}

	return shardsLeaders
}

func roundRobinPolicy(ctx context.Context, getQueryNodePolicy getQueryNodePolicy, query func(UniqueID, types.QueryNode) error, leaders []queryNode) error {
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

		qn, err = getQueryNodePolicy(ctx, leaders[current].address)
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
