package dataservice

import (
	"log"
	"sort"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/distributed/datanode"
)

type (
	dataNode struct {
		id      int64
		address struct {
			ip   string
			port int64
		}
		client     *datanode.Client
		channelNum int
	}
	dataNodeCluster struct {
		mu       sync.RWMutex
		finishCh chan struct{}
		nodes    []*dataNode
	}
)

func newDataNodeCluster(finishCh chan struct{}) *dataNodeCluster {
	return &dataNodeCluster{
		finishCh: finishCh,
		nodes:    make([]*dataNode, 0),
	}
}

func (c *dataNodeCluster) Register(ip string, port int64, id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.checkDataNodeNotExist(ip, port) {
		c.nodes = append(c.nodes, &dataNode{
			id: id,
			address: struct {
				ip   string
				port int64
			}{ip: ip, port: port},
			channelNum: 0,
		})
	}
	if len(c.nodes) == Params.DataNodeNum {
		close(c.finishCh)
	}
}

func (c *dataNodeCluster) checkDataNodeNotExist(ip string, port int64) bool {
	for _, node := range c.nodes {
		if node.address.ip == ip || node.address.port == port {
			return false
		}
	}
	return true
}

func (c *dataNodeCluster) GetNumOfNodes() int {
	return len(c.nodes)
}

func (c *dataNodeCluster) GetNodeIDs() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make([]int64, len(c.nodes))
	for _, node := range c.nodes {
		ret = append(ret, node.id)
	}
	return ret
}

func (c *dataNodeCluster) WatchInsertChannels(groups []channelGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sort.Slice(c.nodes, func(i, j int) bool { return c.nodes[i].channelNum < c.nodes[j].channelNum })
	for i, group := range groups {
		err := c.nodes[i%len(c.nodes)].client.WatchDmChannels(&datapb.WatchDmChannelRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kDescribeCollection,
				MsgID:     -1, // todo
				Timestamp: 0,  // todo
				SourceID:  -1, // todo
			},
			ChannelNames: group,
		})
		if err != nil {
			log.Println(err.Error())
			continue
		}
	}
}

func (c *dataNodeCluster) GetDataNodeStates() ([]*internalpb2.ComponentInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make([]*internalpb2.ComponentInfo, 0)
	for _, node := range c.nodes {
		states, err := node.client.GetComponentStates(nil)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		ret = append(ret, states.State)
	}
	return ret, nil
}

func (c *dataNodeCluster) FlushSegment(request *datapb.FlushSegRequest) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.nodes {
		if err := node.client.FlushSegments(request); err != nil {
			log.Println(err.Error())
			continue
		}
	}
}
