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
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

const clusterPrefix = "cluster-prefix/"
const clusterBuffer = "cluster-buffer"

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
	kv         kv.TxnKV
	dataNodes  map[string]*dataNodeInfo
	chanBuffer []*datapb.ChannelStatus //Unwatched channels buffer
}

func newClusterNodeManager(kv kv.TxnKV) (*clusterNodeManager, error) {
	c := &clusterNodeManager{
		kv:         kv,
		dataNodes:  make(map[string]*dataNodeInfo),
		chanBuffer: []*datapb.ChannelStatus{},
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
	dn, _ := c.kv.Load(clusterBuffer)
	//TODO add not value error check
	if dn != "" {
		info := &datapb.DataNodeInfo{}
		if err := proto.UnmarshalText(dn, info); err != nil {
			return err
		}
		c.chanBuffer = info.Channels
	}

	return nil
}

func (c *clusterNodeManager) updateCluster(dataNodes []*datapb.DataNodeInfo) *clusterDeltaChange {
	newNodes := make([]string, 0)
	offlines := make([]string, 0)
	restarts := make([]string, 0)
	var onCnt, offCnt float64
	currentOnline := make(map[string]struct{})
	for _, n := range dataNodes {
		currentOnline[n.Address] = struct{}{}
		onCnt++
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
		_, has := currentOnline[nAddr]
		if !has && node.status == online {
			node.status = offline
			offCnt++
			offlines = append(offlines, nAddr)
		}
	}
	metrics.DataServiceDataNodeList.WithLabelValues("online").Set(onCnt)
	metrics.DataServiceDataNodeList.WithLabelValues("offline").Set(offCnt)
	return &clusterDeltaChange{
		newNodes: newNodes,
		offlines: offlines,
		restarts: restarts,
	}
}

func (c *clusterNodeManager) updateDataNodes(dataNodes []*datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) error {
	for _, node := range dataNodes {
		c.dataNodes[node.Address].info = node
	}

	return c.txnSaveNodes(dataNodes, buffer)
}

func (c *clusterNodeManager) getDataNodes(onlyOnline bool) (map[string]*datapb.DataNodeInfo, []*datapb.ChannelStatus) {
	ret := make(map[string]*datapb.DataNodeInfo)
	for k, v := range c.dataNodes {
		if !onlyOnline || v.status == online {
			ret[k] = proto.Clone(v.info).(*datapb.DataNodeInfo)
		}
	}
	return ret, c.chanBuffer
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
	c.updateMetrics()
}

func (c *clusterNodeManager) unregister(n *datapb.DataNodeInfo) *datapb.DataNodeInfo {
	node, ok := c.dataNodes[n.Address]
	if !ok {
		return nil
	}
	node.status = offline
	c.updateMetrics()
	return node.info
}

func (c *clusterNodeManager) updateMetrics() {
	var offCnt, onCnt float64
	for _, node := range c.dataNodes {
		if node.status == online {
			onCnt++
		} else {
			offCnt++
		}
	}
	metrics.DataServiceDataNodeList.WithLabelValues("online").Set(onCnt)
	metrics.DataServiceDataNodeList.WithLabelValues("offline").Set(offCnt)
}

func (c *clusterNodeManager) txnSaveNodes(nodes []*datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) error {
	if len(nodes) == 0 && len(buffer) == 0 {
		return nil
	}
	data := make(map[string]string)
	for _, n := range nodes {
		c.dataNodes[n.Address].info = n
		key := clusterPrefix + n.Address
		value := proto.MarshalTextString(n)
		data[key] = value
	}
	c.chanBuffer = buffer

	// short cut, reusing datainfo to store array of channel status
	bufNode := &datapb.DataNodeInfo{
		Channels: buffer,
	}
	data[clusterBuffer] = proto.MarshalTextString(bufNode)
	return c.kv.MultiSave(data)
}
