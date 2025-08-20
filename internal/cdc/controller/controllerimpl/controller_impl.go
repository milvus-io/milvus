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
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
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
	timer := time.NewTicker(checkInterval)
	defer timer.Stop()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
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
		resource.Resource().ReplicateManagerClient().Close()
		close(c.stopChan)
		c.wg.Wait()
		log.Ctx(c.ctx).Info("CDC controller stopped")
	})
}

func (c *controller) run() {
	config, err := resource.Resource().ReplicationCatalog().GetReplicateConfiguration(c.ctx)
	if err != nil {
		log.Ctx(c.ctx).Error("failed to get replicate configuration", zap.Error(err))
		return
	}
	log.Ctx(c.ctx).Info("updating configuration...", wrapConfigLogFields(config)...)
	resource.Resource().ReplicateManagerClient().UpdateReplications(config)
	log.Ctx(c.ctx).Info("configuration updated", wrapConfigLogFields(config)...)
}

func wrapConfigLogFields(config *milvuspb.ReplicateConfiguration) []zap.Field {
	fields := make([]zap.Field, 0)
	fields = append(fields, zap.Int("clusterCount", len(config.GetClusters())))
	fields = append(fields, zap.Strings("clusters", lo.Map(config.GetClusters(), func(cluster *milvuspb.MilvusCluster, _ int) string {
		return cluster.GetClusterID()
	})))
	fields = append(fields, zap.Int("topologyCount", len(config.GetCrossClusterTopology())))
	fields = append(fields, zap.Strings("topologies", lo.Map(config.GetCrossClusterTopology(), func(topology *milvuspb.CrossClusterTopology, _ int) string {
		return fmt.Sprintf("%s->%s", topology.GetSourceClusterID(), topology.GetTargetClusterID())
	})))
	for _, cluster := range config.GetClusters() {
		fields = append(fields, zap.String("clusterInfo", fmt.Sprintf("clusterID: %s, uri: %s, pchannels: %v",
			cluster.GetClusterID(), cluster.GetConnectionParam().GetUri(), cluster.GetPchannels())))
	}
	return fields
}
