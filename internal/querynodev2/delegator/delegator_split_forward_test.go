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

package delegator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A fronted child forwards every delete batch it consumes to its source, with
// each batch's own timestamp, and only after it has released its own delete
// lock, so the source's apply never holds up the child's ingest.
func TestFrontedChildForwardsDeletesToItsSourceAfterUnlocking(t *testing.T) {
	paramtable.Init()

	forwardMock := mockey.Mock((*shardDelegator).forwardStreamingDeletion).Return().Build()
	defer forwardMock.UnPatch()

	child := newTSafeTestDelegator("v1", 0)
	child.deleteBuffer = deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](0, 0, []string{"1", "v1"})

	batches := []DeleteBatch{
		{Ts: 10, Data: []*DeleteData{{PartitionID: 1, PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)}, Timestamps: []uint64{10}, RowCount: 1}}},
		{Ts: 20, Data: []*DeleteData{{PartitionID: 1, PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)}, Timestamps: []uint64{20}, RowCount: 1}}},
	}

	parent := NewMockShardDelegator(t)
	parent.EXPECT().ProcessDeleteBatches(mock.Anything).Run(func(got []DeleteBatch) {
		assert.Equal(t, batches, got, "the source applies the same batches, each with its own ts")
		locked := child.deleteMut.TryLock()
		assert.True(t, locked, "the child's delete lock must be released before forwarding")
		if locked {
			child.deleteMut.Unlock()
		}
	}).Once()
	child.SetFrontingParent(parent)

	child.ProcessDeleteBatches(batches)
}

// A delegator no one fronts forwards nothing.
func TestUnfrontedDelegatorForwardsNoDelete(t *testing.T) {
	paramtable.Init()

	forwardMock := mockey.Mock((*shardDelegator).forwardStreamingDeletion).Return().Build()
	defer forwardMock.UnPatch()

	sd := newTSafeTestDelegator("v0", 0)
	sd.deleteBuffer = deletebuffer.NewListDeleteBuffer[*deletebuffer.Item](0, 0, []string{"1", "v0"})
	sd.ProcessDeleteBatches([]DeleteBatch{{Ts: 10, Data: []*DeleteData{{PartitionID: 1, PrimaryKeys: []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)}, Timestamps: []uint64{10}, RowCount: 1}}}})
	assert.Nil(t, sd.FrontingParent())
}

func TestProcessSplitShardValidatesTheFence(t *testing.T) {
	t.Run("a fence naming no target fronts nothing", func(t *testing.T) {
		sd := newTSafeTestDelegator("v0", 0)
		assert.NoError(t, sd.ProcessSplitShard(context.Background(), nil))
		assert.Empty(t, sd.spawning)
	})

	t.Run("an empty target name is refused before anything is pending", func(t *testing.T) {
		sd := newTSafeTestDelegator("v0", 0)
		err := sd.ProcessSplitShard(context.Background(), []string{"v1", ""})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Empty(t, sd.spawning)
	})
}

func TestSetChildSpawner(t *testing.T) {
	sd := newTSafeTestDelegator("v0", 0)
	spawner := &fakeChildSpawner{}
	sd.SetChildSpawner(spawner)
	assert.Same(t, spawner, sd.childSpawner)
}
