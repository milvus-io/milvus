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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestReplicaQueryVisibility(t *testing.T) {
	replica := NewReplica(&querypb.Replica{
		ID:           10,
		CollectionID: 100,
		Nodes:        []int64{1},
	})
	assert.True(t, replica.IsQueryVisible())

	mutableReplica := replica.CopyForWrite()
	mutableReplica.SetQueryInvisible(true)
	replica = mutableReplica.IntoReplica()
	assert.False(t, replica.IsQueryVisible())

	mutableReplica = replica.CopyForWrite()
	mutableReplica.AddRWNode(2)
	replica = mutableReplica.IntoReplica()
	assert.False(t, replica.IsQueryVisible())
}

func TestReplicaManagerQueryVisibility(t *testing.T) {
	ctx := context.Background()
	manager := NewReplicaManager(nil, nil)

	visibleReplica := NewReplica(&querypb.Replica{
		ID:           10,
		CollectionID: 100,
		Nodes:        []int64{1},
	})
	invisibleReplica := NewReplica(&querypb.Replica{
		ID:           11,
		CollectionID: 100,
		Nodes:        []int64{2},
	})
	mutableReplica := invisibleReplica.CopyForWrite()
	mutableReplica.SetQueryInvisible(true)
	invisibleReplica = mutableReplica.IntoReplica()
	manager.putReplicaInMemory(visibleReplica, invisibleReplica)

	invisibleReplicas := manager.GetQueryInvisibleReplicas(ctx)
	assert.Len(t, invisibleReplicas, 1)
	assert.Equal(t, int64(11), invisibleReplicas[0].GetID())

	collections := manager.SetReplicasQueryVisible(ctx, 11)
	assert.ElementsMatch(t, []int64{100}, collections)
	assert.True(t, manager.Get(ctx, 11).IsQueryVisible())
	assert.Empty(t, manager.GetQueryInvisibleReplicas(ctx))

	manager.putReplicaInMemory(invisibleReplica)
	assert.Len(t, manager.GetQueryInvisibleReplicas(ctx), 1)
	mutableReplica = invisibleReplica.CopyForWrite()
	mutableReplica.SetQueryInvisible(false)
	manager.putReplicaInMemory(mutableReplica.IntoReplica())
	assert.Empty(t, manager.GetQueryInvisibleReplicas(ctx))
}
