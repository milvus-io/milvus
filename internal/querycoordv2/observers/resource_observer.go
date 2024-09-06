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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// ResourceObserver is used to observe resource group status.
// Recover resource group into expected configuration.
type ResourceObserver struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	meta   *meta.Meta

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewResourceObserver(meta *meta.Meta) *ResourceObserver {
	return &ResourceObserver{
		meta: meta,
	}
}

func (ob *ResourceObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.wg.Add(1)
		go ob.schedule(ctx)
	})
}

func (ob *ResourceObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

func (ob *ResourceObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("Start check resource group loop")

	listener := ob.meta.ResourceManager.ListenResourceGroupChanged()
	for {
		ob.waitRGChangedOrTimeout(ctx, listener)
		// stop if the context is canceled.
		if ctx.Err() != nil {
			log.Info("Close resource group observer")
			return
		}

		// do check once.
		ob.checkAndRecoverResourceGroup()
	}
}

func (ob *ResourceObserver) waitRGChangedOrTimeout(ctx context.Context, listener *syncutil.VersionedListener) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, params.Params.QueryCoordCfg.CheckResourceGroupInterval.GetAsDuration(time.Second))
	defer cancel()
	listener.Wait(ctxWithTimeout)
}

func (ob *ResourceObserver) checkAndRecoverResourceGroup() {
	manager := ob.meta.ResourceManager
	rgNames := manager.ListResourceGroups()
	enableRGAutoRecover := params.Params.QueryCoordCfg.EnableRGAutoRecover.GetAsBool()
	log.Debug("start to check resource group", zap.Bool("enableRGAutoRecover", enableRGAutoRecover), zap.Int("resourceGroupNum", len(rgNames)))

	// Check if there is any incoming node.
	if manager.CheckIncomingNodeNum() > 0 {
		log.Info("new incoming node is ready to be assigned...", zap.Int("incomingNodeNum", manager.CheckIncomingNodeNum()))
		manager.AssignPendingIncomingNode()
	}

	log.Debug("recover resource groups...")
	// Recover all resource group into expected configuration.
	for _, rgName := range rgNames {
		if err := manager.MeetRequirement(rgName); err != nil {
			log.Info("found resource group need to be recovered",
				zap.String("rgName", rgName),
				zap.String("reason", err.Error()),
			)

			if enableRGAutoRecover {
				err := manager.AutoRecoverResourceGroup(rgName)
				if err != nil {
					log.Warn("failed to recover resource group",
						zap.String("rgName", rgName),
						zap.Error(err),
					)
				}
			}
		}
	}
	log.Debug("check resource group done", zap.Bool("enableRGAutoRecover", enableRGAutoRecover), zap.Int("resourceGroupNum", len(rgNames)))
}
