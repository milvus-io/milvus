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
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

const clusterPrefix = "cluster-prefix/"

type dataNodeStatus int8

const (
	online dataNodeStatus = iota
	offline
)

type dataNodeInfo struct {
	info   *datapb.DataNodeInfo
	status dataNodeStatus
}

type clusterNodeManager struct {
	kv        kv.TxnKV
	dataNodes map[string]*dataNodeInfo
}

func newClusterNodeManager(kv kv.TxnKV) (*clusterNodeManager, error) {
	c := &clusterNodeManager{
		kv:        kv,
		dataNodes: make(map[string]*dataNodeInfo),
	}
	return c, c.loadFromKv()
}

func (c *clusterNodeManager) loadFromKv() error {
	_, values, err := c.kv.LoadWithPrefix(clusterPrefix)
	if err != nil {
		return err
	}

	for _, v := range values {
		info := &datapb.DataNodeInfo{}
		if err := proto.UnmarshalText(v, info); err != nil {
			return err
		}

		node := &dataNodeInfo{
			info:   info,
			status: offline,
		}
		c.dataNodes[info.Address] = node
	}

	return nil
}

func (c *clusterNodeManager) updateCluster(dataNodes []*datapb.DataNodeInfo) *clusterDeltaChange {
	newNodes := make([]string, 0)
	offlines := make([]string, 0)
	restarts := make([]string, 0)
	for _, n := range dataNodes {
		node, ok := c.dataNodes[n.Address]

		if ok {
			node.status = online
			if node.info.Version != n.Version {
				restarts = append(restarts, n.Address)
			}
			continue
		}

		newNode := &dataNodeInfo{
			info: &datapb.DataNodeInfo{
				Address:  n.Address,
				Version:  n.Version,
				Channels: []*datapb.ChannelStatus{},
			},
			status: online,
		}
		c.dataNodes[n.Address] = newNode
		newNodes = append(newNodes, n.Address)
	}

	for nAddr, node := range c.dataNodes {
		if node.status == offline {
			offlines = append(offlines, nAddr)
		}
	}
	return &clusterDeltaChange{
		newNodes: newNodes,
		offlines: offlines,
		restarts: restarts,
	}
}

func (c *clusterNodeManager) updateDataNodes(dataNodes []*datapb.DataNodeInfo) error {
	for _, node := range dataNodes {
		c.dataNodes[node.Address].info = node
	}

	return c.txnSaveNodes(dataNodes)
}

func (c *clusterNodeManager) getDataNodes(onlyOnline bool) map[string]*datapb.DataNodeInfo {
	ret := make(map[string]*datapb.DataNodeInfo)
	for k, v := range c.dataNodes {
		if !onlyOnline || v.status == online {
			ret[k] = proto.Clone(v.info).(*datapb.DataNodeInfo)
		}
	}
	return ret
}

func (c *clusterNodeManager) register(n *datapb.DataNodeInfo) {
	node, ok := c.dataNodes[n.Address]
	if ok {
		node.status = online
		node.info.Version = n.Version
	} else {
		c.dataNodes[n.Address] = &dataNodeInfo{
			info:   n,
			status: online,
		}
	}
}

func (c *clusterNodeManager) unregister(n *datapb.DataNodeInfo) {
	node, ok := c.dataNodes[n.Address]
	if !ok {
		return
	}
	node.status = offline
}

func (c *clusterNodeManager) txnSaveNodes(nodes []*datapb.DataNodeInfo) error {
	if len(nodes) == 0 {
		return nil
	}
	data := make(map[string]string)
	for _, n := range nodes {
		c.dataNodes[n.Address].info = n
		key := clusterPrefix + n.Address
		value := proto.MarshalTextString(n)
		data[key] = value
	}
	return c.kv.MultiSave(data)
}
