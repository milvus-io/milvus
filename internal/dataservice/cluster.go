// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package dataservice

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
	nodes []*dataNode
}

func (node *dataNode) String() string {
	return fmt.Sprintf("id: %d, address: %s:%d", node.id, node.address.ip, node.address.port)
}

func newDataNodeCluster() *dataNodeCluster {
	return &dataNodeCluster{
		nodes: make([]*dataNode, 0),
	}
}

func (c *dataNodeCluster) Register(dataNode *dataNode) error {
	c.Lock()
	defer c.Unlock()
	if c.checkDataNodeNotExist(dataNode.address.ip, dataNode.address.port) {
		c.nodes = append(c.nodes, dataNode)
		return nil
	}
	return errors.New("datanode already exist")
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
			// ChannelNames: group, // TODO
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
	c.nodes = make([]*dataNode, 0)
}
