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

package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newSplitTestMeta(enableNamespace bool, vchannel string, namespaceRows map[int64]int64) *meta {
	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	partitions := make([]int64, 0, len(namespaceRows))
	for partitionID := range namespaceRows {
		partitions = append(partitions, partitionID)
	}
	m.collections.Insert(1, &collectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Name:            "split_test",
			EnableNamespace: enableNamespace,
		},
		Partitions:    partitions,
		VChannelNames: []string{vchannel},
	})
	segmentID := int64(1000)
	for partitionID, rows := range namespaceRows {
		segmentID++
		m.segments.SetSegment(segmentID, &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  1,
				PartitionID:   partitionID,
				InsertChannel: vchannel,
				State:         commonpb.SegmentState_Flushed,
				NumOfRows:     rows,
				Binlogs: []*datapb.FieldBinlog{
					{Binlogs: []*datapb.Binlog{{LogID: segmentID, MemorySize: rows * 100}}},
				},
			},
		})
	}
	return m
}

func newSplitTestManager(t *testing.T, m *meta) (*shardSplitManager, *mocks.DataCoordCatalog) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, nil).Once()
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(100), nil).Maybe()
	manager, err := newShardSplitManager(context.Background(), m, catalog, alloc, nil, nil, nil)
	assert.NoError(t, err)
	return manager, catalog
}

func TestShardSplitManagerRecovery(t *testing.T) {
	paramtable.Init()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{
		{TaskId: 1, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing, SourceVchannel: "v0"},
		{TaskId: 2, State: datapb.SplitShardTaskState_SplitShardTaskDone, SourceVchannel: "v9"},
	}, nil).Once()
	alloc := allocator.NewMockAllocator(t)

	manager, err := newShardSplitManager(context.Background(), nil, catalog, alloc, nil, nil, nil)
	assert.NoError(t, err)
	// only the unfinished task counts as active.
	assert.Equal(t, 1, manager.activeTaskCount())
	assert.True(t, manager.hasActiveTaskOnVChannel("v0"))
	assert.False(t, manager.hasActiveTaskOnVChannel("v9"))

	// recovery failure surfaces.
	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, errors.New("mock list error")).Once()
	_, err = newShardSplitManager(context.Background(), nil, catalog, alloc, nil, nil, nil)
	assert.Error(t, err)
}

func TestShardSplitManagerDetect(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "true")
	params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "100")
	defer params.Reset(params.DataCoordCfg.ShardSplitEnable.Key)
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardRows.Key)

	// The cluster is not replicating in these cases; stub the balancer lookup so
	// the detection loop neither blocks on the (absent) balancer nor flips on the
	// D6 gate. The replicating path has its own test below.
	notReplicating := mockey.Mock((*shardSplitManager).clusterReplicating).Return(false).Build()
	defer notReplicating.UnPatch()

	t.Run("trigger on multi-namespace shard over rows", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		manager, catalog := newSplitTestManager(t, m)
		var saved *datapb.SplitShardTask
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, task *datapb.SplitShardTask) error {
				saved = task
				return nil
			}).Once()

		manager.detectOnce()
		assert.NotNil(t, saved)
		assert.Equal(t, int64(100), saved.GetTaskId())
		assert.Equal(t, int64(1), saved.GetCollectionId())
		assert.Equal(t, "v0", saved.GetSourceVchannel())
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, saved.GetState())
		assert.Equal(t, 1, manager.activeTaskCount())

		// the shard with an active task is not triggered again.
		manager.detectOnce()
		assert.Equal(t, 1, manager.activeTaskCount())
	})

	t.Run("skip when disabled", func(t *testing.T) {
		params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "false")
		defer params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "true")
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		manager, _ := newSplitTestManager(t, m)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())
	})

	t.Run("skip single-namespace shard", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 200})
		manager, _ := newSplitTestManager(t, m)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())
	})

	t.Run("skip non-namespace collection", func(t *testing.T) {
		m := newSplitTestMeta(false, "v0", map[int64]int64{10: 80, 11: 40})
		manager, _ := newSplitTestManager(t, m)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())
	})

	t.Run("skip under thresholds", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 30, 11: 40})
		manager, _ := newSplitTestManager(t, m)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())
	})

	t.Run("respect concurrency limit", func(t *testing.T) {
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{
			{TaskId: 1, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing, SourceVchannel: "other"},
		}, nil).Once()
		alloc := allocator.NewMockAllocator(t)
		manager, err := newShardSplitManager(context.Background(), m, catalog, alloc, nil, nil, nil)
		assert.NoError(t, err)

		manager.detectOnce()
		// maxConcurrentTasks defaults to 1 and one task is already active.
		assert.Equal(t, 1, manager.activeTaskCount())
	})

	t.Run("task creation failures", func(t *testing.T) {
		// alloc failure.
		m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, nil).Once()
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("mock alloc error")).Once()
		manager, err := newShardSplitManager(context.Background(), m, catalog, alloc, nil, nil, nil)
		assert.NoError(t, err)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())

		// persist failure.
		catalog = mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, nil).Once()
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("mock save error")).Once()
		alloc = allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocID(mock.Anything).Return(int64(100), nil).Once()
		manager, err = newShardSplitManager(context.Background(), m, catalog, alloc, nil, nil, nil)
		assert.NoError(t, err)
		manager.detectOnce()
		assert.Equal(t, 0, manager.activeTaskCount())
	})
}

// TestShardSplitManagerDetectSuppressedWhenReplicating verifies the D6 gate: the
// trigger creates no task while the cluster is replicating, even for a shard that
// is otherwise over the thresholds.
func TestShardSplitManagerDetectSuppressedWhenReplicating(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "true")
	params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "100")
	defer params.Reset(params.DataCoordCfg.ShardSplitEnable.Key)
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardRows.Key)

	replicating := mockey.Mock((*shardSplitManager).clusterReplicating).Return(true).Build()
	defer replicating.UnPatch()

	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 80, 11: 40})
	manager, _ := newSplitTestManager(t, m)
	manager.detectOnce()
	assert.Equal(t, 0, manager.activeTaskCount())
}

func TestShardSplitManagerShouldSplit(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitMaxShardSize.Key, "1") // 1GB
	params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "1000")
	params.Save(params.DataCoordCfg.ShardSplitMaxNamespaceCount.Key, "10")
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardSize.Key)
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardRows.Key)
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxNamespaceCount.Key)

	manager := &shardSplitManager{}
	// every threshold triggers independently, all of them configurable.
	assert.False(t, manager.shouldSplit(&shardStats{size: 1, rows: 1, namespaceCount: 1}))
	assert.True(t, manager.shouldSplit(&shardStats{size: 1 << 30, rows: 1, namespaceCount: 1}))
	assert.True(t, manager.shouldSplit(&shardStats{size: 1, rows: 1000, namespaceCount: 1}))
	assert.True(t, manager.shouldSplit(&shardStats{size: 1, rows: 1, namespaceCount: 10}))
}

func TestShardSplitManagerStartStop(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitCheckInterval.Key, "1")
	defer params.Reset(params.DataCoordCfg.ShardSplitCheckInterval.Key)

	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 1})
	manager, _ := newSplitTestManager(t, m)
	manager.Start()
	manager.Stop()
}
