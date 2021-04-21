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

package proxyservice

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	grpcproxynodeclient "github.com/milvus-io/milvus/internal/distributed/proxynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"
)

type nodeInfo struct {
	ip   string
	port int64
}

type globalNodeInfoTable struct {
	mu      sync.RWMutex
	infos   map[UniqueID]*nodeInfo
	nodeIDs []UniqueID
	// lazy creating, so len(clients) <= len(infos)
	ProxyNodes map[UniqueID]types.ProxyNode
}

func (table *globalNodeInfoTable) randomPick() UniqueID {
	l := len(table.nodeIDs)
	choice := rand.Intn(l)
	return table.nodeIDs[choice]
}

func (table *globalNodeInfoTable) Pick() (*nodeInfo, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

	if len(table.nodeIDs) <= 0 || len(table.infos) <= 0 {
		return nil, errors.New("no available server node")
	}

	id := table.randomPick()
	info, ok := table.infos[id]
	if !ok {
		// though impossible
		return nil, errors.New("fix me, something wrong in pick algorithm")
	}

	return info, nil
}

func (table *globalNodeInfoTable) Register(id UniqueID, info *nodeInfo) error {
	table.mu.Lock()
	defer table.mu.Unlock()

	_, ok := table.infos[id]
	if !ok {
		table.infos[id] = info
	}

	if !funcutil.SliceContain(table.nodeIDs, id) {
		table.nodeIDs = append(table.nodeIDs, id)
	}

	return nil
}

func (table *globalNodeInfoTable) createClients() error {
	if len(table.ProxyNodes) == len(table.infos) {
		return nil
	}

	for nodeID, info := range table.infos {
		_, ok := table.ProxyNodes[nodeID]
		if !ok {
			table.ProxyNodes[nodeID] = grpcproxynodeclient.NewClient(context.Background(), info.ip+":"+strconv.Itoa(int(info.port)))
			var err error
			err = table.ProxyNodes[nodeID].Init()
			if err != nil {
				panic(err)
			}
			err = table.ProxyNodes[nodeID].Start()
			if err != nil {
				panic(err)
			}
		}
	}

	return nil
}

func (table *globalNodeInfoTable) ReleaseAllClients() error {
	table.mu.Lock()
	log.Debug("get write lock")
	defer func() {
		table.mu.Unlock()
		log.Debug("release write lock")
	}()

	var err error
	for id, client := range table.ProxyNodes {
		err = client.Stop()
		if err != nil {
			panic(err)
		}
		delete(table.ProxyNodes, id)
	}

	return nil
}

func (table *globalNodeInfoTable) ObtainAllClients() (map[UniqueID]types.ProxyNode, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

	err := table.createClients()

	return table.ProxyNodes, err
}

func newGlobalNodeInfoTable() *globalNodeInfoTable {
	return &globalNodeInfoTable{
		nodeIDs:    make([]UniqueID, 0),
		infos:      make(map[UniqueID]*nodeInfo),
		ProxyNodes: make(map[UniqueID]types.ProxyNode),
	}
}
