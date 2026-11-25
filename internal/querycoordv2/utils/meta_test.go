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

package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSpawnWithoutWaitRGReadyOption(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr, metacache.NewMetaStore(nil))

	collectionID := int64(3000)
	m.PutCollection(ctx, CreateTestCollection(collectionID, 1))

	m.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})

	// Spawn without WithNeedWaitRGReady — should NOT set the flag
	replicas, err := m.Spawn(ctx, collectionID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	require.Len(t, replicas, 1)
	assert.False(t, replicas[0].NeedWaitRGReady(), "spawn without option should not set NeedWaitRGReady")

	// Spawn with WithNeedWaitRGReady — should set the flag
	replicas2, err := m.Spawn(ctx, collectionID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW, meta.WithNeedWaitRGReady())
	require.NoError(t, err)
	require.Len(t, replicas2, 1)
	assert.True(t, replicas2[0].NeedWaitRGReady(), "spawn with option should set NeedWaitRGReady")
}
