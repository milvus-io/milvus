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

package msgdispatcher

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

type (
	Pos     = msgpb.MsgPosition
	MsgPack = msgstream.MsgPack
	SubPos  = mqwrapper.SubscriptionInitialPosition
)

type Client interface {
	Register(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error)
	Deregister(vchannel string)
	Close()
}

var _ Client = (*client)(nil)

type client struct {
	role      string
	nodeID    int64
	managerMu sync.Mutex
	managers  map[string]DispatcherManager // pchannel -> DispatcherManager
	factory   msgstream.Factory
}

func NewClient(factory msgstream.Factory, role string, nodeID int64) Client {
	return &client{
		role:     role,
		nodeID:   nodeID,
		managers: make(map[string]DispatcherManager),
		factory:  factory,
	}
}

func (c *client) Register(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	c.managerMu.Lock()
	defer c.managerMu.Unlock()
	manager, ok := c.managers[pchannel]
	if !ok {
		manager = NewDispatcherManager(pchannel, c.role, c.nodeID, c.factory)
		c.managers[pchannel] = manager
		go manager.Run()
	}
	ch, err := manager.Add(vchannel, pos, subPos)
	if err != nil {
		if manager.Num() == 0 {
			manager.Close()
			delete(c.managers, pchannel)
		}
		log.Error("register failed", zap.Error(err))
		return nil, err
	}
	log.Info("register done")
	return ch, nil
}

func (c *client) Deregister(vchannel string) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	c.managerMu.Lock()
	defer c.managerMu.Unlock()
	if manager, ok := c.managers[pchannel]; ok {
		manager.Remove(vchannel)
		if manager.Num() == 0 {
			manager.Close()
			delete(c.managers, pchannel)
		}
		log.Info("deregister done", zap.String("role", c.role),
			zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	}
}

func (c *client) Close() {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID))
	c.managerMu.Lock()
	defer c.managerMu.Unlock()
	for pchannel, manager := range c.managers {
		log.Info("close manager", zap.String("channel", pchannel))
		delete(c.managers, pchannel)
		manager.Close()
	}
	log.Info("dispatcher client closed")
}
