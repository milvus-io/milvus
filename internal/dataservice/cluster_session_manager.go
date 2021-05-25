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
	"sync"

	grpcdatanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/types"
)

const retryTimes = 2

type sessionManager interface {
	sendRequest(addr string, executor func(node types.DataNode) error) error
}

type clusterSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]types.DataNode
}

func newClusterSessionManager() *clusterSessionManager {
	return &clusterSessionManager{sessions: make(map[string]types.DataNode)}
}

func (m *clusterSessionManager) createSession(addr string) error {
	cli := grpcdatanodeclient.NewClient(addr)
	if err := cli.Init(); err != nil {
		return err
	}
	if err := cli.Start(); err != nil {
		return err
	}
	m.sessions[addr] = cli
	return nil
}

func (m *clusterSessionManager) getSession(addr string) types.DataNode {
	return m.sessions[addr]
}

func (m *clusterSessionManager) hasSession(addr string) bool {
	_, ok := m.sessions[addr]
	return ok
}

func (m *clusterSessionManager) sendRequest(addr string, executor func(node types.DataNode) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	success := false
	var err error
	for i := 0; !success && i < retryTimes; i++ {
		if i != 0 || !m.hasSession(addr) {
			m.createSession(addr)
		}
		err = executor(m.getSession(addr))
		if err == nil {
			return nil
		}
	}
	return err
}
