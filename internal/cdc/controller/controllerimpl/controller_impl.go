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

package controllerimpl

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const checkInterval = 10 * time.Second

type controller struct {
	ctx      context.Context
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopChan chan struct{}
}

func NewController() *controller {
	return &controller{
		ctx:      context.Background(),
		stopChan: make(chan struct{}),
	}
}

func (c *controller) Start() {
	log.Ctx(c.ctx).Info("CDC controller started")
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		timer := time.NewTicker(checkInterval)
		defer timer.Stop()
		for {
			select {
			case <-c.stopChan:
				return
			case <-timer.C:
				c.run()
			}
		}
	}()
}

func (c *controller) Stop() {
	c.stopOnce.Do(func() {
		log.Ctx(c.ctx).Info("CDC controller stopping...")
		close(c.stopChan)
		c.wg.Wait()
		resource.Resource().ReplicateManagerClient().Close()
		log.Ctx(c.ctx).Info("CDC controller stopped")
	})
}

func (c *controller) run() {
	targetReplicatePChannels, err := resource.Resource().ReplicationCatalog().ListReplicatePChannels(c.ctx)
	if err != nil {
		log.Ctx(c.ctx).Error("failed to get replicate pchannels", zap.Error(err))
		return
	}
	// create replicators for all replicate pchannels
	for _, replicatePChannel := range targetReplicatePChannels {
		resource.Resource().ReplicateManagerClient().CreateReplicator(replicatePChannel)
	}

	// remove out of target replicators
	resource.Resource().ReplicateManagerClient().RemoveOutOfTargetReplicators(targetReplicatePChannels)
}
