package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"go.uber.org/zap"
)

// type pickShardPolicy func(ctx context.Context, mgr *shardClientMgr, query func(UniqueID, types.QueryNode) error, leaders []nodeInfo) error

type pickShardPolicy func(context.Context, *shardClientMgr, func(context.Context, UniqueID, types.QueryNode, []string) error, map[string][]nodeInfo) error

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

// group dml shard leader with same nodeID
func groupShardleadersWithSameQueryNode(
	ctx context.Context,
	shard2leaders map[string][]nodeInfo,
	nexts map[string]int, errSet map[string]error,
	mgr *shardClientMgr) (map[int64][]string, map[int64]types.QueryNode, error) {
	// check if all leaders were checked
	for dml, idx := range nexts {
		if idx >= len(shard2leaders[dml]) {
			log.Warn("no shard leaders were available",
				zap.String("channel", dml),
				zap.String("leaders", fmt.Sprintf("%v", shard2leaders[dml])))
			if e, ok := errSet[dml]; ok {
				return nil, nil, e // return last error recorded
			}
			return nil, nil, fmt.Errorf("no available shard leader")
		}
	}
	qnSet := make(map[int64]types.QueryNode)
	node2dmls := make(map[int64][]string)
	updates := make(map[string]int)

	for dml, idx := range nexts {
		updates[dml] = idx + 1
		nodeInfo := shard2leaders[dml][idx]
		if _, ok := qnSet[nodeInfo.nodeID]; !ok {
			qn, err := mgr.GetClient(ctx, nodeInfo.nodeID)
			if err != nil {
				log.Warn("failed to get shard leader", zap.Int64("nodeID", nodeInfo.nodeID), zap.Error(err))
				// if get client failed, just record error and wait for next round to get client and do query
				errSet[dml] = err
				continue
			}
			qnSet[nodeInfo.nodeID] = qn
		}
		if _, ok := node2dmls[nodeInfo.nodeID]; !ok {
			node2dmls[nodeInfo.nodeID] = make([]string, 0)
		}
		node2dmls[nodeInfo.nodeID] = append(node2dmls[nodeInfo.nodeID], dml)
	}
	// update idxes
	for dml, idx := range updates {
		nexts[dml] = idx
	}
	return node2dmls, qnSet, nil
}

// mergeRoundRobinPolicy first group shard leaders with same querynode, then do the query with multiple dml channels
// if request failed, it finds shard leader for failed dml channels, and again groups shard leaders and do the query
//
// Suppose qn0 is the shard leader for dml-channel0 and dml-channel1, if search for dml-channel0 succeeded, but
// failed for dml-channel1. In this case, an error returned from qn0, and next shard leaders for dml-channel0 and dml-channel1 will be
// retrieved and dml-channel0 therefore will again be searched.
//
// TODO: In this senario, qn0 should return a partial success results for dml-channel0, and only retrys for dml-channel1
func mergeRoundRobinPolicy(
	ctx context.Context,
	mgr *shardClientMgr,
	query func(context.Context, UniqueID, types.QueryNode, []string) error,
	dml2leaders map[string][]nodeInfo) error {
	nexts := make(map[string]int)
	errSet := make(map[string]error) // record err for dml channels
	for dml := range dml2leaders {
		nexts[dml] = 0
	}
	for len(nexts) > 0 {
		node2dmls, nodeset, err := groupShardleadersWithSameQueryNode(ctx, dml2leaders, nexts, errSet, mgr)
		if err != nil {
			return err
		}
		wg := &sync.WaitGroup{}
		mu := &sync.Mutex{}
		wg.Add(len(node2dmls))
		for nodeID, channels := range node2dmls {
			nodeID := nodeID
			channels := channels
			qn := nodeset[nodeID]
			go func() {
				defer wg.Done()
				if err := query(ctx, nodeID, qn, channels); err != nil {
					log.Warn("failed to do query with node", zap.Int64("nodeID", nodeID),
						zap.Strings("dmlChannels", channels), zap.Error(err))
					mu.Lock()
					defer mu.Unlock()
					for _, ch := range channels {
						errSet[ch] = err
					}
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, channel := range channels {
					delete(nexts, channel)
					delete(errSet, channel)
				}
			}()
		}
		wg.Wait()
		if len(nexts) > 0 {
			nextSet := make(map[string]int64)
			for dml, idx := range nexts {
				if idx >= len(dml2leaders[dml]) {
					nextSet[dml] = -1
				} else {
					nextSet[dml] = dml2leaders[dml][idx].nodeID
				}
			}
			log.Warn("retry another query node with round robin", zap.Any("Nexts", nextSet))
		}
	}
	return nil
}
