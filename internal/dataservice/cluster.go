package dataservice

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/types"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type dataNode struct {
	id      int64
	address struct {
		ip   string
		port int64
	}
	client     types.DataNode
	channelNum int
}
type dataNodeCluster struct {
	sync.RWMutex
	finishCh chan struct{}
	nodes    []*dataNode
}

func (node *dataNode) String() string {
	return fmt.Sprintf("id: %d, address: %s:%d", node.id, node.address.ip, node.address.port)
}

func newDataNodeCluster(finishCh chan struct{}) *dataNodeCluster {
	return &dataNodeCluster{
		finishCh: finishCh,
		nodes:    make([]*dataNode, 0),
	}
}

func (c *dataNodeCluster) Register(dataNode *dataNode) {
	c.Lock()
	defer c.Unlock()
	if c.checkDataNodeNotExist(dataNode.address.ip, dataNode.address.port) {
		c.nodes = append(c.nodes, dataNode)
		if len(c.nodes) == Params.DataNodeNum {
			close(c.finishCh)
		}
	}
}

func (c *dataNodeCluster) checkDataNodeNotExist(ip string, port int64) bool {
	for _, node := range c.nodes {
		if node.address.ip == ip && node.address.port == port {
			return false
		}
	}
	return true
}

func (c *dataNodeCluster) GetNumOfNodes() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.nodes)
}

func (c *dataNodeCluster) GetNodeIDs() []int64 {
	c.RLock()
	defer c.RUnlock()
	ret := make([]int64, 0, len(c.nodes))
	for _, node := range c.nodes {
		ret = append(ret, node.id)
	}
	return ret
}

func (c *dataNodeCluster) WatchInsertChannels(channels []string) {
	ctx := context.TODO()
	c.Lock()
	defer c.Unlock()
	var groups [][]string
	if len(channels) < len(c.nodes) {
		groups = make([][]string, len(channels))
	} else {
		groups = make([][]string, len(c.nodes))
	}
	length := len(groups)
	for i, channel := range channels {
		groups[i%length] = append(groups[i%length], channel)
	}
	for i, group := range groups {
		resp, err := c.nodes[i].client.WatchDmChannels(ctx, &datapb.WatchDmChannelsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			ChannelNames: group,
		})
		if err = VerifyResponse(resp, err); err != nil {
			log.Error("watch dm channels error", zap.Stringer("dataNode", c.nodes[i]), zap.Error(err))
			continue
		}
		c.nodes[i].channelNum += len(group)
	}
}

func (c *dataNodeCluster) GetDataNodeStates(ctx context.Context) ([]*internalpb.ComponentInfo, error) {
	c.RLock()
	defer c.RUnlock()
	ret := make([]*internalpb.ComponentInfo, 0)
	for _, node := range c.nodes {
		states, err := node.client.GetComponentStates(ctx)
		if err != nil {
			log.Error("get component states error", zap.Stringer("dataNode", node), zap.Error(err))
			continue
		}
		ret = append(ret, states.State)
	}
	return ret, nil
}

func (c *dataNodeCluster) FlushSegment(request *datapb.FlushSegmentsRequest) {
	ctx := context.TODO()
	c.Lock()
	defer c.Unlock()
	for _, node := range c.nodes {
		if _, err := node.client.FlushSegments(ctx, request); err != nil {
			log.Error("flush segment err", zap.Stringer("dataNode", node), zap.Error(err))
			continue
		}
	}
}

func (c *dataNodeCluster) ShutDownClients() {
	c.Lock()
	defer c.Unlock()
	for _, node := range c.nodes {
		if err := node.client.Stop(); err != nil {
			log.Error("stop client error", zap.Stringer("dataNode", node), zap.Error(err))
			continue
		}
	}
}

// Clear only for test
func (c *dataNodeCluster) Clear() {
	c.Lock()
	defer c.Unlock()
	c.finishCh = make(chan struct{})
	c.nodes = make([]*dataNode, 0)
}
