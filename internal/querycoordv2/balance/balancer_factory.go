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

package balance

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// BalancerFactory is responsible for creating and caching balancer instances.
// It supports dynamic balancer switching based on configuration changes.
type BalancerFactory struct {
	balancerMap          map[string]Balance
	stoppingBalancerMap  map[string]*StoppingBalancer
	balancerLock         sync.RWMutex
	stoppingBalancerLock sync.RWMutex

	// Dependencies for creating balancers
	scheduler   task.Scheduler
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	targetMgr   meta.TargetManagerInterface
}

// Global factory instance
var (
	globalFactory *BalancerFactory
	factoryOnce   sync.Once
)

// InitGlobalBalancerFactory initializes the global balancer factory singleton.
// This should be called once during server startup.
func InitGlobalBalancerFactory(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) {
	factoryOnce.Do(func() {
		globalFactory = NewBalancerFactory(scheduler, nodeManager, dist, meta, targetMgr)
		log.Info("Global balancer factory initialized")
	})
}

// GetGlobalBalancerFactory returns the global balancer factory instance.
// Returns nil if InitGlobalBalancerFactory has not been called.
func GetGlobalBalancerFactory() *BalancerFactory {
	return globalFactory
}

// ResetGlobalBalancerFactoryForTest resets the global factory for testing purposes.
// This should only be used in tests.
func ResetGlobalBalancerFactoryForTest() {
	globalFactory = nil
	factoryOnce = sync.Once{}
}

// NewBalancerFactory creates a new BalancerFactory instance.
func NewBalancerFactory(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *BalancerFactory {
	return &BalancerFactory{
		balancerMap:         make(map[string]Balance),
		stoppingBalancerMap: make(map[string]*StoppingBalancer),
		scheduler:           scheduler,
		nodeManager:         nodeManager,
		dist:                dist,
		meta:                meta,
		targetMgr:           targetMgr,
	}
}

// GetBalancer returns a balancer instance based on the current configuration.
// It caches balancer instances and reuses them when the configuration hasn't changed.
func (f *BalancerFactory) GetBalancer() Balance {
	balanceKey := paramtable.Get().QueryCoordCfg.Balancer.GetValue()

	f.balancerLock.Lock()
	defer f.balancerLock.Unlock()

	balancer, ok := f.balancerMap[balanceKey]
	if ok {
		return balancer
	}

	log.Info("Creating new balancer", zap.String("type", balanceKey))

	switch balanceKey {
	case meta.RoundRobinBalancerName:
		balancer = NewRoundRobinBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	case meta.RowCountBasedBalancerName:
		balancer = NewRowCountBasedBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	case meta.ScoreBasedBalancerName:
		balancer = NewScoreBasedBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	case meta.MultiTargetBalancerName:
		balancer = NewMultiTargetBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	case meta.ChannelLevelScoreBalancerName:
		balancer = NewChannelLevelScoreBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	default:
		log.Info("Unknown balancer type, using default",
			zap.String("requested", balanceKey),
			zap.String("default", meta.ChannelLevelScoreBalancerName))
		balancer = NewChannelLevelScoreBalancer(f.scheduler, f.nodeManager, f.dist, f.meta, f.targetMgr)
	}

	f.balancerMap[balanceKey] = balancer
	return balancer
}

// GetStoppingBalancer returns a stopping balancer instance based on the current configuration.
// It caches balancer instances by policy type and reuses them when the configuration hasn't changed.
func (f *BalancerFactory) GetStoppingBalancer() *StoppingBalancer {
	policyType := paramtable.Get().QueryCoordCfg.StoppingBalanceAssignPolicy.GetValue()

	f.stoppingBalancerLock.Lock()
	defer f.stoppingBalancerLock.Unlock()

	balancer, ok := f.stoppingBalancerMap[policyType]
	if ok {
		return balancer
	}

	log.Info("Creating new stopping balancer", zap.String("policyType", policyType))

	// Use AssignPolicyFactory to get cached policy instance
	assignPolicy := assign.GetGlobalAssignPolicyFactory().GetPolicy(policyType)

	balancer = NewStoppingBalancer(f.dist, f.targetMgr, assignPolicy, f.nodeManager)

	f.stoppingBalancerMap[policyType] = balancer
	return balancer
}
