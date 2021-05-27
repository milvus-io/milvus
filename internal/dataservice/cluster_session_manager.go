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

	"github.com/milvus-io/milvus/internal/types"
)

const retryTimes = 2

type sessionManager interface {
	getOrCreateSession(addr string) (types.DataNode, error)
	releaseSession(addr string)
	release()
}

type clusterSessionManager struct {
	mu                sync.RWMutex
	sessions          map[string]types.DataNode
	dataClientCreator func(addr string) (types.DataNode, error)
}

func newClusterSessionManager(dataClientCreator func(addr string) (types.DataNode, error)) *clusterSessionManager {
	return &clusterSessionManager{
		sessions:          make(map[string]types.DataNode),
		dataClientCreator: dataClientCreator,
	}
}

func (m *clusterSessionManager) createSession(addr string) error {
	cli, err := m.dataClientCreator(addr)
	if err != nil {
		return err
	}
	if err := cli.Init(); err != nil {
		return err
	}
	if err := cli.Start(); err != nil {
		return err
	}
	m.sessions[addr] = cli
	return nil
}

func (m *clusterSessionManager) getOrCreateSession(addr string) (types.DataNode, error) {
	if !m.hasSession(addr) {
		if err := m.createSession(addr); err != nil {
			return nil, err
		}
	}
	return m.sessions[addr], nil
}

func (m *clusterSessionManager) hasSession(addr string) bool {
	_, ok := m.sessions[addr]
	return ok
}

func (m *clusterSessionManager) releaseSession(addr string) {
	cli, ok := m.sessions[addr]
	if !ok {
		return
	}
	_ = cli.Stop()
	delete(m.sessions, addr)
}

func (m *clusterSessionManager) release() {
	for _, cli := range m.sessions {
		_ = cli.Stop()
	}
}
