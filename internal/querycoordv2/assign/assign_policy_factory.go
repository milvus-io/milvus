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

package assign

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	// PolicyTypeRoundRobin uses simple round-robin assignment
	PolicyTypeRoundRobin = "round_robin"
	// PolicyTypeRowCount uses row count/channel count-based priority queue assignment
	PolicyTypeRowCount = "row_count"
	// PolicyTypeScoreBased uses comprehensive score-based assignment with benefit evaluation
	PolicyTypeScoreBased = "score_based"
)

// AssignPolicyFactory is responsible for creating and caching assign policy instances.
// It supports dynamic policy switching based on configuration changes.
type AssignPolicyFactory struct {
	policyMap  map[string]AssignPolicy
	policyLock sync.RWMutex

	// Dependencies for creating policies
	scheduler   task.Scheduler
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	targetMgr   meta.TargetManagerInterface
}

// Global factory instance
var (
	globalPolicyFactory *AssignPolicyFactory
	policyFactoryOnce   sync.Once
)

// InitGlobalAssignPolicyFactory initializes the global assign policy factory singleton.
// This should be called once during server startup.
func InitGlobalAssignPolicyFactory(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) {
	policyFactoryOnce.Do(func() {
		globalPolicyFactory = NewAssignPolicyFactory(scheduler, nodeManager, dist, meta, targetMgr)
		log.Info("Global assign policy factory initialized")
	})
}

// GetGlobalAssignPolicyFactory returns the global assign policy factory instance.
// Returns nil if InitGlobalAssignPolicyFactory has not been called.
func GetGlobalAssignPolicyFactory() *AssignPolicyFactory {
	return globalPolicyFactory
}

// ResetGlobalAssignPolicyFactoryForTest resets the global factory for testing purposes.
// This should only be used in tests.
func ResetGlobalAssignPolicyFactoryForTest() {
	globalPolicyFactory = nil
	policyFactoryOnce = sync.Once{}
}

// NewAssignPolicyFactory creates a new AssignPolicyFactory instance.
func NewAssignPolicyFactory(
	scheduler task.Scheduler,
	nodeManager *session.NodeManager,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
) *AssignPolicyFactory {
	return &AssignPolicyFactory{
		policyMap:   make(map[string]AssignPolicy),
		scheduler:   scheduler,
		nodeManager: nodeManager,
		dist:        dist,
		meta:        meta,
		targetMgr:   targetMgr,
	}
}

// GetPolicy returns an assign policy instance based on the specified policy type.
// It caches policy instances and reuses them when the policy type hasn't changed.
func (f *AssignPolicyFactory) GetPolicy(policyType string) AssignPolicy {
	f.policyLock.Lock()
	defer f.policyLock.Unlock()

	policy, ok := f.policyMap[policyType]
	if ok {
		return policy
	}

	log.Info("Creating new assign policy", zap.String("type", policyType))

	switch policyType {
	case PolicyTypeRoundRobin:
		policy = newRoundRobinAssignPolicy(f.nodeManager, f.scheduler, f.targetMgr)
	case PolicyTypeRowCount:
		policy = newRowCountBasedAssignPolicy(f.nodeManager, f.scheduler, f.dist)
	case PolicyTypeScoreBased:
		policy = newScoreBasedAssignPolicy(f.nodeManager, f.scheduler, f.dist, f.meta)
	default:
		log.Info("Unknown assign policy type, using default",
			zap.String("requested", policyType),
			zap.String("default", PolicyTypeScoreBased))
		policy = newScoreBasedAssignPolicy(f.nodeManager, f.scheduler, f.dist, f.meta)
	}

	f.policyMap[policyType] = policy
	return policy
}
