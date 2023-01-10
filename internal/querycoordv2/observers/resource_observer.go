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

package observers

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"go.uber.org/zap"
)

// check whether rg lack of node, try to transfer node from default rg
type ResourceObserver struct {
	c    chan struct{}
	wg   sync.WaitGroup
	meta *meta.Meta

	stopOnce sync.Once
}

func NewResourceObserver(meta *meta.Meta) *ResourceObserver {
	return &ResourceObserver{
		c:    make(chan struct{}),
		meta: meta,
	}
}

func (ob *ResourceObserver) Start(ctx context.Context) {
	ob.wg.Add(1)
	go ob.schedule(ctx)
}

func (ob *ResourceObserver) Stop() {
	ob.stopOnce.Do(func() {
		close(ob.c)
		ob.wg.Wait()
	})
}

func (ob *ResourceObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start check resource group loop")

	ticker := time.NewTicker(params.Params.QueryCoordCfg.CheckResourceGroupInterval.GetAsDuration(time.Second))
	for {
		select {
		case <-ctx.Done():
			log.Info("Close resource group observer due to context canceled")
			return
		case <-ob.c:
			log.Info("Close resource group observer")
			return

		case <-ticker.C:
			ob.checkResourceGroup()
		}
	}
}

func (ob *ResourceObserver) checkResourceGroup() {
	manager := ob.meta.ResourceManager
	rgNames := manager.ListResourceGroups()

	enableRGAutoRecover := params.Params.QueryCoordCfg.EnableRGAutoRecover.GetAsBool()

	for _, rgName := range rgNames {
		if rgName == meta.DefaultResourceGroupName {
			continue
		}
		lackNodeNum := manager.CheckLackOfNode(rgName)
		if lackNodeNum > 0 {
			log.Info("found resource group lack of nodes",
				zap.String("rgName", rgName),
				zap.Int("lackNodeNum", lackNodeNum),
			)

			if enableRGAutoRecover {
				usedNodeNum, err := manager.AutoRecoverResourceGroup(rgName)
				if err != nil {
					log.Warn("failed to recover resource group",
						zap.String("rgName", rgName),
						zap.Int("lackNodeNum", lackNodeNum-usedNodeNum),
						zap.Error(err),
					)
				}
			}
		}
	}
}
