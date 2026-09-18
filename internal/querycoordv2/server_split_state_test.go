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

package querycoordv2

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	kvmocks "github.com/milvus-io/milvus/internal/kv/mocks"
	coordMocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
)

// TestInitMetaAndObserverShareSplitState pins the server wiring of the shard
// split state cache without starting a server: initMeta builds one cache and
// hands it to the target manager (window marks), and initObserver hands the same
// cache to the target observer (window-end refresh).
func TestInitMetaAndObserverShareSplitState(t *testing.T) {
	// an empty meta store.
	store := kvmocks.NewMetaKv(t)
	store.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	store.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil).Maybe()
	store.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil).Maybe()

	var targetMgrCache, observerCache *meta.ShardSplitStateCache
	var newTargetMgr func(meta.Broker, *meta.Meta, *meta.ShardSplitStateCache) *meta.TargetManager
	mockTargetMgr := mockey.Mock(meta.NewTargetManagerWithSplitState).To(
		func(broker meta.Broker, m *meta.Meta, splitStates *meta.ShardSplitStateCache) *meta.TargetManager {
			targetMgrCache = splitStates
			return newTargetMgr(broker, m, splitStates)
		}).Origin(&newTargetMgr).Build()
	defer mockTargetMgr.UnPatch()

	var newObserver func(*meta.Meta, meta.TargetManagerInterface, *meta.DistributionManager, meta.Broker,
		session.Cluster, *session.NodeManager, *meta.ShardSplitStateCache) *observers.TargetObserver
	mockObserver := mockey.Mock(observers.NewTargetObserverWithSplitState).To(
		func(m *meta.Meta, targetMgr meta.TargetManagerInterface, dist *meta.DistributionManager, broker meta.Broker,
			cluster session.Cluster, nodeMgr *session.NodeManager, splitState *meta.ShardSplitStateCache,
		) *observers.TargetObserver {
			observerCache = splitState
			return newObserver(m, targetMgr, dist, broker, cluster, nodeMgr, splitState)
		}).Origin(&newObserver).Build()
	defer mockObserver.UnPatch()

	server := createSimpleTestServer()
	server.kv = store
	server.mixCoord = coordMocks.NewMixCoord(t)

	assert.NoError(t, server.initMeta())
	server.initObserver()

	assert.NotNil(t, server.splitState)
	assert.NotNil(t, server.targetMgr)
	assert.NotNil(t, server.targetObserver)
	assert.Same(t, server.splitState, targetMgrCache)
	assert.Same(t, server.splitState, observerCache)
}
