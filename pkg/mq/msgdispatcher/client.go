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
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	Pos     = msgpb.MsgPosition
	MsgPack = msgstream.MsgPack
	SubPos  = common.SubscriptionInitialPosition
)

type StreamConfig struct {
	VChannel        string
	Pos             *Pos
	SubPos          SubPos
	ReplicateConfig *msgstream.ReplicateConfig
}

func NewStreamConfig(vchannel string, pos *Pos, subPos SubPos) *StreamConfig {
	return &StreamConfig{
		VChannel: vchannel,
		Pos:      pos,
		SubPos:   subPos,
	}
}

type Client interface {
	Register(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error)
	Deregister(vchannel string)
	Close()
}

var _ Client = (*client)(nil)

type client struct {
	role       string
	nodeID     int64
	managers   *typeutil.ConcurrentMap[string, DispatcherManager]
	managerMut *lock.KeyLock[string]
	factory    msgstream.Factory
}

func NewClient(factory msgstream.Factory, role string, nodeID int64) Client {
	return &client{
		role:       role,
		nodeID:     nodeID,
		factory:    factory,
		managers:   typeutil.NewConcurrentMap[string, DispatcherManager](),
		managerMut: lock.NewKeyLock[string](),
	}
}

func (c *client) Register(ctx context.Context, streamConfig *StreamConfig) (<-chan *MsgPack, error) {
	start := time.Now()
	vchannel := streamConfig.VChannel
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	log := log.Ctx(ctx).With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID), zap.String("vchannel", vchannel))

	c.managerMut.Lock(pchannel)
	defer c.managerMut.Unlock(pchannel)

	var manager DispatcherManager
	manager, ok := c.managers.Get(pchannel)
	if !ok {
		manager = NewDispatcherManager(pchannel, c.role, c.nodeID, c.factory)
		c.managers.Insert(pchannel, manager)
		go manager.Run()
	}

	// Begin to register
	ch, err := manager.Add(ctx, streamConfig)
	if err != nil {
		log.Error("register failed", zap.Error(err))
		return nil, err
	}
	log.Info("register done", zap.Duration("dur", time.Since(start)))
	return ch, nil
}

func (c *client) Deregister(vchannel string) {
	start := time.Now()
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	c.managerMut.Lock(pchannel)
	defer c.managerMut.Unlock(pchannel)

	if manager, ok := c.managers.Get(pchannel); ok {
		manager.Remove(vchannel)
		if manager.NumTarget() == 0 && manager.NumConsumer() == 0 {
			manager.Close()
			c.managers.Remove(pchannel)
		}
		log.Info("deregister done", zap.String("role", c.role), zap.Int64("nodeID", c.nodeID),
			zap.String("vchannel", vchannel), zap.Duration("dur", time.Since(start)))
	}
}

func (c *client) Close() {
	log := log.With(zap.String("role", c.role), zap.Int64("nodeID", c.nodeID))

	c.managers.Range(func(pchannel string, manager DispatcherManager) bool {
		c.managerMut.Lock(pchannel)
		defer c.managerMut.Unlock(pchannel)

		log.Info("close manager", zap.String("channel", pchannel))
		c.managers.Remove(pchannel)
		manager.Close()
		return true
	})
	log.Info("dispatcher client closed")
}
