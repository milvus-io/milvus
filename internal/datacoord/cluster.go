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

package datacoord

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type cluster struct {
	mu               sync.RWMutex
	ctx              context.Context
	dataManager      *clusterNodeManager
	sessionManager   sessionManager
	candidateManager *candidateManager
	posProvider      positionProvider

	startupPolicy    clusterStartupPolicy
	registerPolicy   dataNodeRegisterPolicy
	unregisterPolicy dataNodeUnregisterPolicy
	assignPolicy     channelAssignPolicy
}

type clusterOption struct {
	apply func(c *cluster)
}

func withStartupPolicy(p clusterStartupPolicy) clusterOption {
	return clusterOption{
		apply: func(c *cluster) { c.startupPolicy = p },
	}
}

func withRegisterPolicy(p dataNodeRegisterPolicy) clusterOption {
	return clusterOption{
		apply: func(c *cluster) { c.registerPolicy = p },
	}
}

func withUnregistorPolicy(p dataNodeUnregisterPolicy) clusterOption {
	return clusterOption{
		apply: func(c *cluster) { c.unregisterPolicy = p },
	}
}

func withAssignPolicy(p channelAssignPolicy) clusterOption {
	return clusterOption{
		apply: func(c *cluster) { c.assignPolicy = p },
	}
}

func defaultStartupPolicy() clusterStartupPolicy {
	return newWatchRestartsStartupPolicy()
}

func defaultRegisterPolicy() dataNodeRegisterPolicy {
	return newAssiggBufferRegisterPolicy()
}

func defaultUnregisterPolicy() dataNodeUnregisterPolicy {
	return randomAssignRegisterFunc
}

func defaultAssignPolicy() channelAssignPolicy {
	return newBalancedAssignPolicy()
}

func newCluster(ctx context.Context, dataManager *clusterNodeManager,
	sessionManager sessionManager, posProvider positionProvider,
	opts ...clusterOption) *cluster {
	c := &cluster{
		ctx:              ctx,
		sessionManager:   sessionManager,
		dataManager:      dataManager,
		posProvider:      posProvider,
		startupPolicy:    defaultStartupPolicy(),
		registerPolicy:   defaultRegisterPolicy(),
		unregisterPolicy: defaultUnregisterPolicy(),
		assignPolicy:     defaultAssignPolicy(),
	}

	c.candidateManager = newCandidateManager(20, c.validateDataNode, c.enableDataNode)

	for _, opt := range opts {
		opt.apply(c)
	}

	return c
}

// startup applies statup policy
func (c *cluster) startup(dataNodes []*datapb.DataNodeInfo) error {
	/*deltaChange := c.dataManager.updateCluster(dataNodes)
	nodes, chanBuffer := c.dataManager.getDataNodes(false)
	var rets []*datapb.DataNodeInfo
	var err error
	rets, chanBuffer = c.startupPolicy.apply(nodes, deltaChange, chanBuffer)
	c.dataManager.updateDataNodes(rets, chanBuffer)
	rets, err = c.watch(rets)
	if err != nil {
		log.Warn("Failed to watch all the status change", zap.Error(err))
		//does not trigger new another refresh, pending evt will do
	}
	c.dataManager.updateDataNodes(rets, chanBuffer)
	return nil*/
	return c.refresh(dataNodes)
}

// refresh rough refresh datanode status after event received
func (c *cluster) refresh(dataNodes []*datapb.DataNodeInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	deltaChange := c.dataManager.updateCluster(dataNodes)
	log.Debug("refresh delta", zap.Any("new", deltaChange.newNodes),
		zap.Any("restart", deltaChange.restarts),
		zap.Any("offline", deltaChange.offlines))

	// cannot use startup policy directly separate into three parts:
	// 1. add new nodes into candidates list
	for _, dn := range dataNodes {
		for _, newAddr := range deltaChange.newNodes {
			if dn.Address == newAddr {
				c.candidateManager.add(dn)
			}
		}
	}

	// 2. restart nodes, disable node&session, execute unregister policy and put node into candidate list
	restartNodes := make([]*datapb.DataNodeInfo, 0, len(deltaChange.restarts))
	for _, node := range deltaChange.restarts {
		info, ok := c.dataManager.dataNodes[node]
		if ok {
			restartNodes = append(restartNodes, info.info)
			c.dataManager.unregister(node) // remove from cluster
			c.sessionManager.releaseSession(node)
		} else {
			log.Warn("Restart node not in node manager", zap.String("restart_node", node))
		}
	}
	if len(restartNodes) > 0 {
		for _, node := range restartNodes {
			cluster, buffer := c.dataManager.getDataNodes(true)
			if len(cluster) > 0 {
				ret := c.unregisterPolicy.apply(cluster, node)
				c.updateNodeWatch(ret, buffer)
			} else {
				// no online node, put all watched channels to buffer
				buffer = append(buffer, node.Channels...)
				c.updateNodeWatch([]*datapb.DataNodeInfo{}, buffer)
			}
			node.Channels = node.Channels[:0] // clear channels
			c.candidateManager.add(node)      // put node into candidate list
		}
	}

	// 3. offline do unregister
	unregisterNodes := make([]*datapb.DataNodeInfo, 0, len(deltaChange.offlines)) // possible nodes info to unregister
	for _, node := range deltaChange.offlines {
		c.sessionManager.releaseSession(node)
		info := c.dataManager.unregister(node)
		if info != nil {
			unregisterNodes = append(unregisterNodes, info)
		}
	}
	for _, node := range unregisterNodes {
		cluster, buffer := c.dataManager.getDataNodes(true)
		if len(cluster) > 0 { // cluster has online nodes, migrate channels
			ret := c.unregisterPolicy.apply(cluster, node)
			c.updateNodeWatch(ret, buffer)
		} else {
			// no online node, put all watched channels to buffer
			buffer = append(buffer, node.Channels...)
			c.updateNodeWatch([]*datapb.DataNodeInfo{}, buffer)
		}
	}

	return nil
}

// updateNodeWatch save nodes uncomplete status and try to watch channels which is unwatched, save the execution result
func (c *cluster) updateNodeWatch(nodes []*datapb.DataNodeInfo, buffer []*datapb.ChannelStatus) error {
	c.dataManager.updateDataNodes(nodes, buffer)
	rets, err := c.watch(nodes)
	if err != nil {
		log.Warn("Failed to watch all the status change", zap.Error(err)) //
	}
	c.dataManager.updateDataNodes(rets, buffer)
	return err
}

// paraRun parallel run, with max Parallel limit
func paraRun(works []func(), maxRunner int) {
	wg := sync.WaitGroup{}
	ch := make(chan func())
	wg.Add(len(works))
	if maxRunner > len(works) {
		maxRunner = len(works)
	}

	for i := 0; i < maxRunner; i++ {
		go func() {
			work, ok := <-ch
			if !ok {
				return
			}
			work()
			wg.Done()
		}()
	}
	for _, work := range works {
		ch <- work
	}
	wg.Wait()
	close(ch)
}

func (c *cluster) validateDataNode(dn *datapb.DataNodeInfo) error {
	log.Warn("[CM] start validate candidate", zap.String("addr", dn.Address))
	_, err := c.sessionManager.getOrCreateSession(dn.Address) // this might take time if address went offline
	log.Warn("[CM] candidate validation finished", zap.String("addr", dn.Address), zap.Error(err))
	if err != nil {
		return err
	}
	return nil
}

func (c *cluster) enableDataNode(dn *datapb.DataNodeInfo) error {
	log.Warn("[CM] enabling candidate", zap.String("addr", dn.Address))
	c.register(dn)
	return nil
}

func (c *cluster) watch(nodes []*datapb.DataNodeInfo) ([]*datapb.DataNodeInfo, error) {
	works := make([]func(), 0, len(nodes))
	mut := sync.Mutex{}
	errs := make([]error, 0, len(nodes))
	for _, n := range nodes {
		works = append(works, func() {
			logMsg := fmt.Sprintf("Begin to watch channels for node %s, channels:", n.Address)
			uncompletes := make([]vchannel, 0, len(n.Channels))
			for _, ch := range n.Channels {
				if ch.State == datapb.ChannelWatchState_Uncomplete {
					if len(uncompletes) == 0 {
						logMsg += ch.Name
					} else {
						logMsg += "," + ch.Name
					}
					uncompletes = append(uncompletes, vchannel{
						CollectionID: ch.CollectionID,
						DmlChannel:   ch.Name,
					})
				}
			}

			if len(uncompletes) == 0 {
				return // all set, just return
			}
			log.Debug(logMsg)

			vchanInfos, err := c.posProvider.GetVChanPositions(uncompletes, true)
			if err != nil {
				log.Warn("get vchannel position failed", zap.Error(err))
				mut.Lock()
				errs = append(errs, err)
				mut.Unlock()
				return
			}
			cli, err := c.sessionManager.getSession(n.Address) //fail fast, don't create session
			if err != nil {
				log.Warn("get session failed", zap.String("addr", n.Address), zap.Error(err))
				mut.Lock()
				errs = append(errs, err)
				mut.Unlock()
				return
			}
			req := &datapb.WatchDmChannelsRequest{
				Base: &commonpb.MsgBase{
					SourceID: Params.NodeID,
				},
				Vchannels: vchanInfos,
			}
			resp, err := cli.WatchDmChannels(c.ctx, req)
			if err != nil {
				log.Warn("watch dm channel failed", zap.String("addr", n.Address), zap.Error(err))
				mut.Lock()
				errs = append(errs, err)
				mut.Unlock()
				return
			}
			if resp.ErrorCode != commonpb.ErrorCode_Success {
				log.Warn("watch channels failed", zap.String("address", n.Address), zap.Error(err))
				mut.Lock()
				errs = append(errs, fmt.Errorf("watch fail with stat %v, msg:%s", resp.ErrorCode, resp.Reason))
				mut.Unlock()
				return
			}
			for _, ch := range n.Channels {
				if ch.State == datapb.ChannelWatchState_Uncomplete {
					ch.State = datapb.ChannelWatchState_Complete
				}
			}
		})
	}
	paraRun(works, 20)
	if len(errs) > 0 {
		return nodes, retry.ErrorList(errs)
	}
	return nodes, nil
}

func (c *cluster) register(n *datapb.DataNodeInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataManager.register(n)
	cNodes, chanBuffer := c.dataManager.getDataNodes(true)
	var rets []*datapb.DataNodeInfo
	var err error
	log.Debug("before register policy applied", zap.Any("n.Channels", n.Channels), zap.Any("buffer", chanBuffer))
	rets, chanBuffer = c.registerPolicy.apply(cNodes, n, chanBuffer)
	log.Debug("after register policy applied", zap.Any("ret", rets), zap.Any("buffer", chanBuffer))
	c.dataManager.updateDataNodes(rets, chanBuffer)
	rets, err = c.watch(rets)
	if err != nil {
		log.Warn("Failed to watch all the status change", zap.Error(err))
		//does not trigger new another refresh, pending evt will do
	}
	c.dataManager.updateDataNodes(rets, chanBuffer)
}

func (c *cluster) unregister(n *datapb.DataNodeInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sessionManager.releaseSession(n.Address)
	oldNode := c.dataManager.unregister(n.Address)
	if oldNode != nil {
		n = oldNode
	}
	cNodes, chanBuffer := c.dataManager.getDataNodes(true)
	log.Debug("before unregister policy applied", zap.Any("n.Channels", n.Channels), zap.Any("buffer", chanBuffer))
	var rets []*datapb.DataNodeInfo
	var err error
	if len(cNodes) == 0 {
		for _, chStat := range n.Channels {
			chStat.State = datapb.ChannelWatchState_Uncomplete
			chanBuffer = append(chanBuffer, chStat)
		}
	} else {
		rets = c.unregisterPolicy.apply(cNodes, n)
	}
	log.Debug("after register policy applied", zap.Any("ret", rets), zap.Any("buffer", chanBuffer))
	c.dataManager.updateDataNodes(rets, chanBuffer)
	rets, err = c.watch(rets)
	if err != nil {
		log.Warn("Failed to watch all the status change", zap.Error(err))
		//does not trigger new another refresh, pending evt will do
	}
	c.dataManager.updateDataNodes(rets, chanBuffer)
}

func (c *cluster) watchIfNeeded(channel string, collectionID UniqueID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cNodes, chanBuffer := c.dataManager.getDataNodes(true)
	var rets []*datapb.DataNodeInfo
	var err error
	if len(cNodes) == 0 { // no nodes to assign, put into buffer
		chanBuffer = append(chanBuffer, &datapb.ChannelStatus{
			Name:         channel,
			CollectionID: collectionID,
			State:        datapb.ChannelWatchState_Uncomplete,
		})
	} else {
		rets = c.assignPolicy.apply(cNodes, channel, collectionID)
	}
	c.dataManager.updateDataNodes(rets, chanBuffer)
	rets, err = c.watch(rets)
	if err != nil {
		log.Warn("Failed to watch all the status change", zap.Error(err))
		//does not trigger new another refresh, pending evt will do
	}
	c.dataManager.updateDataNodes(rets, chanBuffer)
}

func (c *cluster) flush(segments []*datapb.SegmentInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := make(map[string]map[UniqueID][]UniqueID) // channel-> map[collectionID]segmentIDs

	for _, seg := range segments {
		if _, ok := m[seg.InsertChannel]; !ok {
			m[seg.InsertChannel] = make(map[UniqueID][]UniqueID)
		}

		m[seg.InsertChannel][seg.CollectionID] = append(m[seg.InsertChannel][seg.CollectionID], seg.ID)
	}

	dataNodes, _ := c.dataManager.getDataNodes(true)

	channel2Node := make(map[string]string)
	for _, node := range dataNodes {
		for _, chstatus := range node.Channels {
			channel2Node[chstatus.Name] = node.Address
		}
	}

	for ch, coll2seg := range m {
		node, ok := channel2Node[ch]
		if !ok {
			continue
		}
		cli, err := c.sessionManager.getSession(node)
		if err != nil {
			log.Warn("get session failed", zap.String("addr", node), zap.Error(err))
			continue
		}
		for coll, segs := range coll2seg {
			req := &datapb.FlushSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType:  commonpb.MsgType_Flush,
					SourceID: Params.NodeID,
				},
				CollectionID: coll,
				SegmentIDs:   segs,
			}
			resp, err := cli.FlushSegments(c.ctx, req)
			if err != nil {
				log.Warn("flush segment failed", zap.String("addr", node), zap.Error(err))
				continue
			}
			if resp.ErrorCode != commonpb.ErrorCode_Success {
				log.Warn("flush segment failed", zap.String("dataNode", node), zap.Error(err))
				continue
			}
			log.Debug("flush segments succeed", zap.Any("segmentIDs", segs))
		}
	}
}

func (c *cluster) releaseSessions() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionManager.release()
	c.candidateManager.dispose()
}
