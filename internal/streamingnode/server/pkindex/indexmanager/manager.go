// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package indexmanager is the primary key index of a streaming node, at the
// pchannel level and above. A Manager holds a pchannel for each
// pchannel. A pchannel keeps the member table of its vchannels,
// takes the writes and the TimeTicks of the observe of the WAL recovery,
// drives the handovers and keeps the LastAcked bound. Each vchannel is an
// index.Index (package index), which owns the engine of the vchannel and the
// write path decider on top of it (package internal/pkindex/dedup),
// and hands its tables over to object storage through a Committer.
package indexmanager

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Manager owns the primary key indexes of one streaming node. It opens one
// pchannel per pchannel.
//
// The Manager does not own ManagerConfig.Engine. The caller releases it after
// Close returns.
type managerImpl struct {
	cfg       ManagerConfig
	lifetime  *typeutil.Lifetime
	closeOnce sync.Once

	mu       sync.Mutex
	channels map[string]*pchannel // by pchannel name
}

// NewManager creates a Manager. It panics when cfg.Committer is nil: an
// index that can not commit is never recovered, so a reopen of the WAL
// would lose the writes before its checkpoint.
func NewManager(cfg ManagerConfig) Manager {
	if cfg.Committer == nil {
		panic("the primary key index manager needs a Committer")
	}
	return &managerImpl{
		cfg:      cfg,
		lifetime: typeutil.NewLifetime(),
		channels: make(map[string]*pchannel),
	}
}

// New creates the primary key index of a pchannel and registers it, so that
// Close closes it with the node. It does not start the handover driver.
//
// The owner, the WAL recovery, opens the indexes of the vchannels with an
// enabled primary key index with OpenIndex, recovers them, and then calls
// Start.
// It closes the pchannel when the WAL closes.
//
// After Close, New returns a closed PChannel: OpenIndex and RecoverIndexes
// return an error that matches engine.ErrClosed.
func (m *managerImpl) New(cfg PChannelConfig) PChannel {
	c := newPChannel(m, cfg)
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		c.Close()
		return c
	}
	defer m.lifetime.Done()
	m.mu.Lock()
	m.channels[cfg.PChannel.Name] = c
	m.mu.Unlock()
	return c
}

// forget removes a closed pchannel from the Manager.
func (m *managerImpl) forget(c *pchannel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.channels[c.info.Name] == c {
		delete(m.channels, c.info.Name)
	}
}

// Close closes every open pchannel. It does not release ManagerConfig.Engine.
func (m *managerImpl) Close() {
	m.closeOnce.Do(m.close)
}

func (m *managerImpl) close() {
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.lifetime.Wait()

	m.mu.Lock()
	channels := make([]*pchannel, 0, len(m.channels))
	for _, c := range m.channels {
		channels = append(channels, c)
	}
	m.mu.Unlock()
	for _, c := range channels {
		c.Close()
	}
}

var _ Manager = (*managerImpl)(nil)
