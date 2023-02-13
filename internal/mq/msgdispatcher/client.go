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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type (
	Pos     = internalpb.MsgPosition
	MsgPack = msgstream.MsgPack
	SubPos  = mqwrapper.SubscriptionInitialPosition
)

type Client interface {
	Register(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error)
	Deregister(vchannel string)
}

var _ Client = (*client)(nil)

type client struct {
	role     string
	nodeID   int64
	managers *typeutil.ConcurrentMap[string, DispatcherManager] // pchannel -> DispatcherManager
	factory  msgstream.Factory
}

func NewClient(factory msgstream.Factory, role string, nodeID int64) Client {
	return &client{
		role:     role,
		nodeID:   nodeID,
		managers: typeutil.NewConcurrentMap[string, DispatcherManager](),
		factory:  factory,
	}
}

func (c *client) Register(vchannel string, pos *Pos, subPos SubPos) (<-chan *MsgPack, error) {
	log := log.With(zap.String("role", c.role),
		zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	managers, ok := c.managers.Get(pchannel)
	if !ok {
		managers = NewDispatcherManager(pchannel, c.role, c.nodeID, c.factory)
		go managers.Run()
		old, exist := c.managers.GetOrInsert(pchannel, managers)
		if exist {
			managers.Close()
			managers = old
		}
	}
	ch, err := managers.Add(vchannel, pos, subPos)
	if err != nil {
		log.Error("register failed", zap.Error(err))
		return nil, err
	}
	log.Info("register done")
	return ch, nil
}

func (c *client) Deregister(vchannel string) {
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	if managers, ok := c.managers.Get(pchannel); ok {
		managers.Remove(vchannel)
		if managers.Num() == 0 {
			managers.Close()
			c.managers.GetAndRemove(pchannel)
		}
		log.Info("deregister done", zap.String("role", c.role),
			zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))
	}
}
