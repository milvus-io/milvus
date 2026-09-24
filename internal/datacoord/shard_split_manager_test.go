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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

const (
	splitMgrCollection = int64(1)
	splitMgrV0         = "by-dev-rootcoord-dml_0_1v0"
	splitMgrV1         = "by-dev-rootcoord-dml_1_1v1"
	splitMgrV2         = "by-dev-rootcoord-dml_2_1v2"
	splitMgrV3         = "by-dev-rootcoord-dml_3_1v3"
	splitMgrV4         = "by-dev-rootcoord-dml_4_1v4"
	splitMgrControl    = "by-dev-rootcoord-dml_9_vcchan"
)

// splitTestSchema is a collection placed by its int64 primary key.
func splitTestSchema(enableNamespace bool) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:            "coll",
		EnableNamespace: enableNamespace,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
		},
	}
}

// splitTestDescribe is rootcoord's answer for a collection with the given
// vchannels, shard infos (nil for a never-split collection) and modulus.
func splitTestDescribe(vchannels []string, infos []*schemapb.CollectionShardInfo, modulus uint64) *milvuspb.DescribeCollectionResponse {
	pchannels := make([]string, 0, len(vchannels))
	for _, vchannel := range vchannels {
		pchannels = append(pchannels, vchannel[:len("by-dev-rootcoord-dml_0")])
	}
	return &milvuspb.DescribeCollectionResponse{
		Status:               merr.Success(),
		CollectionID:         splitMgrCollection,
		CollectionName:       "coll",
		DbName:               "db",
		Schema:               splitTestSchema(false),
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: pchannels,
		ShardInfos:           infos,
		RoutingModulus:       modulus,
	}
}

func hashInfo(vchannel string, state schemapb.ShardState, buckets ...uint64) *schemapb.CollectionShardInfo {
	info := &schemapb.CollectionShardInfo{VchannelName: vchannel, State: state}
	if len(buckets) > 0 {
		info.Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: buckets}}
	}
	return info
}

// fakeSplitCoordinator stands in for the datacoord server the manager acts
// through.
type fakeSplitCoordinator struct {
	mu          sync.Mutex
	coll        *splitCollection
	describeErr error
	issueErr    error
	onIssue     func(task *datapb.SplitShardTask)
	issued      []*datapb.SplitShardTask
	drainReason string
	fenceReason string
	adoptErr    error
	adopted     []*datapb.SplitShardTask
	served      bool
	servedErr   error
}

func (f *fakeSplitCoordinator) issueShardSplitAdoption(_ context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if controlChannel != splitMgrControl {
		return errors.New("wrong control channel")
	}
	f.adopted = append(f.adopted, task)
	return f.adoptErr
}

func (f *fakeSplitCoordinator) splitSourceServed(context.Context, int64, string) (bool, error) {
	return f.served, f.servedErr
}

func (f *fakeSplitCoordinator) describeSplitCollection(_ context.Context, _ int64) (*splitCollection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	return f.coll, nil
}

func (f *fakeSplitCoordinator) issueShardSplit(_ context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	f.mu.Lock()
	f.issued = append(f.issued, task)
	onIssue, err := f.onIssue, f.issueErr
	f.mu.Unlock()
	if controlChannel != splitMgrControl {
		return errors.New("wrong control channel")
	}
	if onIssue != nil {
		onIssue(task)
	}
	return err
}

func (f *fakeSplitCoordinator) splitDrainBlockReason(context.Context, *datapb.SplitShardTask) string {
	return f.drainReason
}

func (f *fakeSplitCoordinator) fenceFlushBlockReason(*datapb.SplitShardTask) string {
	return f.fenceReason
}

// fakeVChannelAllocator records what it was asked and continues the index the
// way the streaming allocator does.
type fakeVChannelAllocator struct {
	params []balancer.AllocVChannelParam
	err    error
	result []string
}

func (f *fakeVChannelAllocator) AllocVirtualChannels(_ context.Context, param balancer.AllocVChannelParam) ([]string, error) {
	f.params = append(f.params, param)
	return f.result, f.err
}

type fakeRedistributor struct{ rounds []int64 }

func (f *fakeRedistributor) redistribute(_ context.Context, task *datapb.SplitShardTask) {
	f.rounds = append(f.rounds, task.GetTaskId())
}

func newSplitTestManager(t *testing.T, coordinator *fakeSplitCoordinator) (*shardSplitManager, *fakeVChannelAllocator) {
	t.Helper()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	manager := newShardSplitManager(context.Background(), m, newMockAllocator(t), newShardSplitTasks(), coordinator)
	vchannels := &fakeVChannelAllocator{result: []string{splitMgrV1, splitMgrV2}}
	manager.vchannelAllocator = vchannels
	manager.controlChannel = func() string { return splitMgrControl }
	manager.replicationRole = func(context.Context) (replicateutil.Role, error) { return replicateutil.RolePrimary, nil }
	// Issues run in step, so a test observes each one's effect on the next line.
	manager.spawn = func(issue func()) { issue() }
	return manager, vchannels
}

// enableShardSplit turns on the switch and sets a small row threshold.
func enableShardSplit(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "true")
	params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "1000")
	params.Save(params.DataCoordCfg.EnableCompaction.Key, "true")
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.ShardSplitEnable.Key)
		params.Reset(params.DataCoordCfg.ShardSplitMaxShardRows.Key)
		params.Reset(params.DataCoordCfg.EnableCompaction.Key)
	})
}

func addSplitTestCollection(m *meta, schema *schemapb.CollectionSchema, vchannels ...string) {
	m.AddCollection(&collectionInfo{ID: splitMgrCollection, Schema: schema, VChannelNames: vchannels, Partitions: []int64{10}})
}

func addSplitTestSegment(m *meta, id int64, vchannel string, rows, size int64) {
	m.segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            id,
		CollectionID:  splitMgrCollection,
		PartitionID:   10,
		InsertChannel: vchannel,
		State:         commonpb.SegmentState_Flushed,
		NumOfRows:     rows,
		Stats:         &datapb.Statistics{InsertBinlogSize: size},
	}})
}

func TestResiduesOfAndThePlan(t *testing.T) {
	t.Run("a never-split collection owns residue i on shard i", func(t *testing.T) {
		coll := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1}, nil, 0), []int64{10})
		residues, err := residuesOf(coll.Collection)
		require.NoError(t, err)
		assert.EqualValues(t, 2, residues.modulus)
		assert.Equal(t, []uint64{0}, residues.byVChannel[splitMgrV0])
		assert.Equal(t, []uint64{1}, residues.byVChannel[splitMgrV1])
		owner, ok := residues.ownerOf(1)
		assert.True(t, ok)
		assert.Equal(t, splitMgrV1, owner)
		_, ok = residues.ownerOf(7)
		assert.False(t, ok)
	})

	t.Run("a split collection lists its writable shards only", func(t *testing.T) {
		coll := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
			hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
			hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 1),
		}, 2), []int64{10})
		residues, err := residuesOf(coll.Collection)
		require.NoError(t, err)
		_, err = residues.of(splitMgrV0)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		own, err := residues.of(splitMgrV2)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1}, own)
	})

	t.Run("a topology that does not tile is refused", func(t *testing.T) {
		coll := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardNormal, 0),
			hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 0),
		}, 2), nil)
		_, err := residuesOf(coll.Collection)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	t.Run("a single residue doubles the modulus", func(t *testing.T) {
		left, right, after, err := planSplitResidues(1, []uint64{0})
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, left)
		assert.Equal(t, []uint64{1}, right)
		assert.EqualValues(t, 2, after)

		left, right, after, err = planSplitResidues(4, []uint64{3})
		require.NoError(t, err)
		assert.Equal(t, []uint64{3}, left)
		assert.Equal(t, []uint64{7}, right)
		assert.EqualValues(t, 8, after)
	})

	t.Run("a set is halved at the same modulus, the extra residue on the left", func(t *testing.T) {
		left, right, after, err := planSplitResidues(8, []uint64{6, 0, 2})
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 2}, left)
		assert.Equal(t, []uint64{6}, right)
		assert.EqualValues(t, 8, after)
	})

	t.Run("malformed plans are refused", func(t *testing.T) {
		for _, tc := range []struct {
			modulus uint64
			own     []uint64
		}{
			{0, []uint64{0}},
			{maxSplitModulus + 1, []uint64{0}},
			{maxSplitModulus, []uint64{0}},
			{2, nil},
			{2, []uint64{2}},
			{4, []uint64{1, 1}},
		} {
			_, _, _, err := planSplitResidues(tc.modulus, tc.own)
			assert.ErrorIs(t, err, merr.ErrServiceInternal, "modulus %d own %v", tc.modulus, tc.own)
		}
	})

	t.Run("rebase re-expresses a residue on the larger modulus", func(t *testing.T) {
		rebased, err := rebaseResidues([]uint64{1}, 2, 8)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1, 3, 5, 7}, rebased)
		_, err = rebaseResidues([]uint64{1}, 3, 8)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		_, err = rebaseResidues([]uint64{1}, 0, 8)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestSplittableCollection(t *testing.T) {
	assert.False(t, splittableCollection(nil))
	assert.False(t, splittableCollection(&collectionInfo{}))
	assert.True(t, splittableCollection(&collectionInfo{Schema: splitTestSchema(false)}))
	// A namespace collection is never selected (design doc §1.3), in either
	// namespace.mode and whatever its sharding property says.
	assert.False(t, splittableCollection(&collectionInfo{
		Schema:     splitTestSchema(true),
		Properties: map[string]string{common.NamespaceShardingEnabledKey: "true"},
	}))
	external := splitTestSchema(false)
	external.Fields[1].ExternalField = "embedding"
	assert.False(t, splittableCollection(&collectionInfo{Schema: external}))
	// The rewrite writer is not TEXT-aware (LOB references): a collection
	// with a TEXT field is never selected, or every rewrite plan would fail.
	assert.False(t, splittableCollection(&collectionInfo{Schema: splitTestSchemaWithText()}))
	assert.Equal(t, "the collection has a TEXT field, which a shard split rewrite cannot carry yet",
		splitRefusalReason(splitTestSchemaWithText()))
	assert.Empty(t, splitRefusalReason(splitTestSchema(false)))
	assert.Empty(t, splitRefusalReason(nil))
	// Every field the writer writes is looked at, struct sub-fields included.
	nested := splitTestSchema(false)
	nested.StructArrayFields = []*schemapb.StructArrayFieldSchema{{
		Name:   "st",
		Fields: []*schemapb.FieldSchema{{FieldID: 103, Name: "st_doc", DataType: schemapb.DataType_Text}},
	}}
	assert.NotEmpty(t, splitRefusalReason(nested))
}

// splitTestSchemaWithText is splitTestSchema with a TEXT field.
func splitTestSchemaWithText() *schemapb.CollectionSchema {
	schema := splitTestSchema(false)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "doc", DataType: schemapb.DataType_Text})
	return schema
}

// The record wins over datacoord's cache: a TEXT field rootcoord reports is
// refused at planning even if the cached schema lacked it.
func TestShardSplitPlanRefusesACollectionWithATextField(t *testing.T) {
	desc := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	desc.Schema = splitTestSchemaWithText()
	coordinator := &fakeSplitCoordinator{coll: splitCollectionFromDescribe(desc, []int64{10})}
	manager, _ := newSplitTestManager(t, coordinator)
	task, err := manager.planSplit(splitMgrCollection, &shardStats{vchannel: splitMgrV0})
	assert.NoError(t, err)
	assert.Nil(t, task)
	assert.Empty(t, manager.store.list())
}

// A TEXT field added between planning and the target allocation aborts the
// task: nothing of it is in any WAL yet.
func TestShardSplitPreparingAbortsOnATextField(t *testing.T) {
	manager, coordinator, vchannels := newPreparingCase(t)
	desc := splitTestDescribe([]string{splitMgrV0}, nil, 0)
	desc.Schema = splitTestSchemaWithText()
	coordinator.coll = splitCollectionFromDescribe(desc, []int64{10})

	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, task.GetState())
	assert.Contains(t, task.GetFailReason(), "TEXT")
	assert.Empty(t, vchannels.params, "no target is allocated")
	assert.Empty(t, coordinator.issued)
}

func TestShardSplitTriggerPlansAnOverThresholdShard(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
	addSplitTestSegment(manager.meta, 1, splitMgrV0, 600, 1)
	addSplitTestSegment(manager.meta, 2, splitMgrV0, 600, 1)

	manager.detectOnce()
	tasks := manager.store.list()
	require.Len(t, tasks, 1)
	task := tasks[0]
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, task.GetState())
	assert.Equal(t, splitMgrCollection, task.GetCollectionId())
	assert.Equal(t, splitMgrV0, splitTaskSource(task))
	// The first split of a never-split shard doubles the modulus.
	assert.EqualValues(t, 2, task.GetRoutingModulus())
	require.Len(t, task.GetTargets(), 2)
	assert.Equal(t, []uint64{0}, task.GetTargets()[0].GetBuckets())
	assert.Equal(t, []uint64{1}, task.GetTargets()[1].GetBuckets())
	assert.Empty(t, task.GetTargets()[0].GetVchannel(), "targets are allocated in Preparing")
	assert.False(t, task.GetFenced())

	// The shard is now named by an active task: the trigger does not fire on
	// it again.
	manager.detectOnce()
	assert.Len(t, manager.store.list(), 1)
}

func TestShardSplitTriggerSkipsACollectionWithATextField(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchemaWithText(), splitMgrV0)
	addSplitTestSegment(manager.meta, 1, splitMgrV0, 5000, 1)

	manager.detectOnce()
	assert.Empty(t, manager.store.list())
}

func TestShardSplitTriggerGates(t *testing.T) {
	newTriggerCase := func(t *testing.T) (*shardSplitManager, *fakeSplitCoordinator) {
		enableShardSplit(t)
		coordinator := &fakeSplitCoordinator{
			coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
		}
		manager, _ := newSplitTestManager(t, coordinator)
		addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
		addSplitTestSegment(manager.meta, 1, splitMgrV0, 5000, 1)
		return manager, coordinator
	}

	t.Run("a shard under both thresholds is left alone", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.ShardSplitMaxShardRows.Key, "10000")
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("the size threshold alone triggers too", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		params := paramtable.Get()
		params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "10000")
		params.Save(params.DataCoordCfg.ShardSplitMaxShardSize.Key, "0")
		defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardSize.Key)
		manager.detectOnce()
		assert.Len(t, manager.store.list(), 1)
	})

	t.Run("the switch off issues nothing", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.ShardSplitEnable.Key, "false")
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("compaction off issues nothing", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		manager.compactionEnabled = false
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("the compaction switch is the one read at startup", func(t *testing.T) {
		// dataCoord.enableCompaction is not refreshable: the trigger follows
		// the value the policy-driven compactions started with, not a later
		// change nothing else follows.
		params := paramtable.Get()
		enableShardSplit(t)
		params.Save(params.DataCoordCfg.EnableCompaction.Key, "false")
		coordinator := &fakeSplitCoordinator{
			coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
		}
		manager, _ := newSplitTestManager(t, coordinator)
		addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
		addSplitTestSegment(manager.meta, 1, splitMgrV0, 5000, 1)
		params.Save(params.DataCoordCfg.EnableCompaction.Key, "true")
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("a secondary never plans a split", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		manager.replicationRole = func(context.Context) (replicateutil.Role, error) { return replicateutil.RoleSecondary, nil }
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("an unknown replication role counts as a secondary", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		manager.replicationRole = func(context.Context) (replicateutil.Role, error) {
			return replicateutil.RolePrimary, errors.New("balancer not ready")
		}
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("the concurrency cap counts every task that is not terminal", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		require.NoError(t, manager.store.create(context.Background(), manager.catalog, &datapb.SplitShardTask{
			TaskId: 900, CollectionId: 7, State: datapb.SplitShardTaskState_SplitShardTaskAdopting,
			Sources: []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_5_7v0"}},
		}))
		manager.detectOnce()
		assert.Len(t, manager.store.list(), 1)
	})

	t.Run("a Done task frees its slot and its vchannels", func(t *testing.T) {
		manager, _ := newTriggerCase(t)
		require.NoError(t, manager.store.create(context.Background(), manager.catalog, &datapb.SplitShardTask{
			TaskId: 900, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskDone,
			Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV0}},
		}))
		manager.detectOnce()
		assert.Len(t, manager.store.list(), 2)
	})

	t.Run("a vchannel that is a target of an active task is skipped", func(t *testing.T) {
		// A vchannel belongs to one collection, so a task naming it is a task
		// of that collection, and the one-split-per-collection guard skips it.
		manager, _ := newTriggerCase(t)
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.ShardSplitMaxConcurrentTasks.Key, "5")
		defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ShardSplitMaxConcurrentTasks.Key)
		require.NoError(t, manager.store.create(context.Background(), manager.catalog, &datapb.SplitShardTask{
			TaskId: 900, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
			Sources: []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_5_7v0"}},
			Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV0}, {Vchannel: "by-dev-rootcoord-dml_6_7v1"}},
		}))
		manager.detectOnce()
		assert.Len(t, manager.store.list(), 1)
	})

	t.Run("a namespace collection is never selected", func(t *testing.T) {
		manager, coordinator := newTriggerCase(t)
		addSplitTestCollection(manager.meta, splitTestSchema(true), splitMgrV0)
		manager.detectOnce()
		assert.Empty(t, manager.store.list())

		// Nor when only rootcoord's record says it is one.
		addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
		coordinator.coll.EnableNamespace = true
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("a shard that is not Normal in rootcoord's record is left alone", func(t *testing.T) {
		manager, coordinator := newTriggerCase(t)
		coordinator.coll = splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardCreating, 0),
		}, 1), []int64{10})
		manager.detectOnce()
		assert.Empty(t, manager.store.list())

		coordinator.coll = splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV1}, nil, 0), []int64{10})
		manager.detectOnce()
		assert.Empty(t, manager.store.list(), "a vchannel rootcoord no longer lists is not split")
	})

	t.Run("a failed read of the collection plans nothing", func(t *testing.T) {
		manager, coordinator := newTriggerCase(t)
		coordinator.describeErr = merr.WrapErrServiceUnavailableMsg("rootcoord busy")
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})

	t.Run("a routing that does not tile plans nothing", func(t *testing.T) {
		manager, coordinator := newTriggerCase(t)
		coordinator.coll = splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardNormal, 0),
		}, 2), []int64{10})
		manager.detectOnce()
		assert.Empty(t, manager.store.list())
	})
}

// The runaway-doubling guard: a shard whose last doubling left its sibling
// half nearly empty is not doubled again.
func TestShardSplitTriggerRefusesAnUnrelievedDoubling(t *testing.T) {
	enableShardSplit(t)
	infos := []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardNormal, 1),
	}
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV1, splitMgrV2}, infos, 2), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV1, splitMgrV2)
	addSplitTestSegment(manager.meta, 1, splitMgrV1, 5000, 1000)
	addSplitTestSegment(manager.meta, 2, splitMgrV2, 1, 1)

	manager.detectOnce()
	assert.Empty(t, manager.store.list(), "v1's sibling half holds almost nothing")

	// Disabled, the same shard is doubled.
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitMinSiblingRatio.Key, "0")
	defer params.Reset(params.DataCoordCfg.ShardSplitMinSiblingRatio.Key)
	manager.detectOnce()
	require.Len(t, manager.store.list(), 1)
	assert.EqualValues(t, 4, manager.store.list()[0].GetRoutingModulus())
}

func TestDoublingRelievedNothing(t *testing.T) {
	enableShardSplit(t)
	manager, _ := newSplitTestManager(t, &fakeSplitCoordinator{})
	coll := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
		hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 0),
		hashInfo(splitMgrV2, schemapb.ShardState_ShardNormal, 1),
	}, 2), nil)
	residues, err := residuesOf(coll.Collection)
	require.NoError(t, err)
	addSplitTestSegment(manager.meta, 1, splitMgrV2, 1, 500)

	assert.False(t, manager.doublingRelievedNothing(coll, residues, []uint64{0}, 0), "an empty shard")
	assert.False(t, manager.doublingRelievedNothing(coll, residues, []uint64{0, 2}, 1000), "a set is not doubled")
	assert.False(t, manager.doublingRelievedNothing(coll, residues, []uint64{0}, 1000), "the sibling holds half")
	assert.True(t, manager.doublingRelievedNothing(coll, residues, []uint64{0}, 100000))

	// A never-split collection has no doubling in its ancestry.
	never := splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV1, splitMgrV2}, nil, 0), nil)
	assert.False(t, manager.doublingRelievedNothing(never, residues, []uint64{0}, 100000))

	// A sibling nobody owns means nothing is on the other half.
	orphan := &shardResidues{modulus: 2, byVChannel: map[string][]uint64{splitMgrV1: {0}}}
	assert.True(t, manager.doublingRelievedNothing(coll, orphan, []uint64{0}, 100000))

	_, ok := siblingResidue(3, 1)
	assert.False(t, ok)
}

// preparingTask is a planned task: residues and modulus decided, targets not
// allocated.
func preparingTask() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:         100,
		CollectionId:   splitMgrCollection,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV0}},
		Targets:        []*datapb.SplitShardTaskTarget{{Buckets: []uint64{0}}, {Buckets: []uint64{1}}},
		RoutingModulus: 2,
		StartTime:      uint64(time.Now().Unix()),
	}
}

func newPreparingCase(t *testing.T) (*shardSplitManager, *fakeSplitCoordinator, *fakeVChannelAllocator) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, vchannels := newSplitTestManager(t, coordinator)
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, preparingTask()))
	return manager, coordinator, vchannels
}

func mustTask(t *testing.T, manager *shardSplitManager, id int64) *datapb.SplitShardTask {
	t.Helper()
	task, ok := manager.store.get(id)
	require.True(t, ok)
	return task
}

func TestShardSplitPreparingAllocatesAndIssuesTheWriteSwitch(t *testing.T) {
	manager, coordinator, vchannels := newPreparingCase(t)

	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
	assert.Equal(t, []string{splitMgrV1, splitMgrV2}, splitTaskTargetVChannels(task))
	require.Len(t, coordinator.issued, 1)
	assert.Equal(t, []string{splitMgrV1, splitMgrV2}, splitTaskTargetVChannels(coordinator.issued[0]))
	// Nothing the ack callback owns is written by the manager.
	assert.False(t, task.GetFenced())
	assert.Zero(t, task.GetSources()[0].GetSwitchTimeTick())

	require.Len(t, vchannels.params, 1)
	assert.Equal(t, splitMgrCollection, vchannels.params[0].CollectionID)
	assert.Equal(t, 2, vchannels.params[0].Num)
	assert.Equal(t, []string{splitMgrV0}, vchannels.params[0].ExistingVChannels)

	// Fencing waits for the callback; nothing is issued again.
	manager.advanceTasks()
	assert.Len(t, coordinator.issued, 1)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, mustTask(t, manager, 100).GetState())
}

// Target names are never reused: the allocator is told every vchannel the
// collection lists AND every name another unfinished split of it has already
// allocated but not yet listed, so the shard index continues after all of them.
func TestShardSplitTargetNamesContinueAfterEveryKnownVChannel(t *testing.T) {
	manager, coordinator, vchannels := newPreparingCase(t)
	coordinator.coll = splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1}, nil, 0), []int64{10})
	ctx := context.Background()
	// Another split of the same collection holds v3 and v4, persisted before
	// its write switch lists them.
	require.NoError(t, manager.store.create(ctx, manager.catalog, &datapb.SplitShardTask{
		TaskId: 101, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV1}},
		Targets: []*datapb.SplitShardTaskTarget{{Vchannel: splitMgrV3}, {Vchannel: splitMgrV4}},
	}))
	// A finished split of it and a split of another collection add nothing.
	require.NoError(t, manager.store.create(ctx, manager.catalog, &datapb.SplitShardTask{
		TaskId: 102, CollectionId: splitMgrCollection, State: datapb.SplitShardTaskState_SplitShardTaskDone,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_7_1v7"}},
	}))
	require.NoError(t, manager.store.create(ctx, manager.catalog, &datapb.SplitShardTask{
		TaskId: 103, CollectionId: 2, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: "by-dev-rootcoord-dml_8_2v8"}},
	}))
	vchannels.result = []string{"by-dev-rootcoord-dml_5_1v5", "by-dev-rootcoord-dml_6_1v6"}

	task := mustTask(t, manager, 100)
	manager.advanceTask(task)
	require.Len(t, vchannels.params, 1)
	assert.Equal(t, []string{splitMgrV0, splitMgrV1, splitMgrV3, splitMgrV4}, vchannels.params[0].ExistingVChannels)
	assert.Equal(t, []string{"by-dev-rootcoord-dml_5_1v5", "by-dev-rootcoord-dml_6_1v6"},
		splitTaskTargetVChannels(mustTask(t, manager, 100)))
}

func TestShardSplitPreparingFailures(t *testing.T) {
	t.Run("an allocation failure aborts the task before anything is fenced", func(t *testing.T) {
		manager, coordinator, vchannels := newPreparingCase(t)
		vchannels.err = errors.New("not enough pchannels")
		manager.advanceTasks()
		task := mustTask(t, manager, 100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, task.GetState())
		assert.Contains(t, task.GetFailReason(), "not enough pchannels")
		assert.NotZero(t, task.GetEndTime())
		assert.Empty(t, coordinator.issued)
		// Aborted is terminal: the task is never advanced again.
		manager.advanceTasks()
		assert.Len(t, vchannels.params, 1)
	})

	t.Run("an allocator that returns the wrong count aborts", func(t *testing.T) {
		manager, _, vchannels := newPreparingCase(t)
		vchannels.result = []string{splitMgrV1}
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, mustTask(t, manager, 100).GetState())
	})

	t.Run("a collection dropped before the allocation aborts", func(t *testing.T) {
		manager, coordinator, _ := newPreparingCase(t)
		coordinator.describeErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskAborted, mustTask(t, manager, 100).GetState())
	})

	t.Run("a failed read of the collection waits", func(t *testing.T) {
		manager, coordinator, vchannels := newPreparingCase(t)
		coordinator.describeErr = merr.WrapErrServiceUnavailableMsg("rootcoord busy")
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mustTask(t, manager, 100).GetState())
		assert.Empty(t, vchannels.params)
	})

	t.Run("a failed write switch stays in Preparing with the same targets", func(t *testing.T) {
		manager, coordinator, vchannels := newPreparingCase(t)
		coordinator.issueErr = merr.WrapErrServiceUnavailableMsg("broadcaster not ready")
		manager.advanceTasks()
		manager.advanceTasks()
		task := mustTask(t, manager, 100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, task.GetState())
		assert.Len(t, coordinator.issued, 2, "every tick re-issues the same write switch")
		assert.Len(t, vchannels.params, 1, "the targets are allocated once")
		assert.Equal(t, []string{splitMgrV1, splitMgrV2}, splitTaskTargetVChannels(task))

		// Once targets are persisted a write switch may be in the WAL: no abort.
		manager.abortTask(task, "too late")
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mustTask(t, manager, 100).GetState())
	})

	t.Run("a collection dropped at the write switch finishes the task", func(t *testing.T) {
		manager, coordinator, _ := newPreparingCase(t)
		coordinator.issueErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
		manager.advanceTasks()
		task := mustTask(t, manager, 100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
		assert.Equal(t, "collection dropped before the write switch", task.GetFailReason())
	})

	t.Run("compaction off still fences a created task", func(t *testing.T) {
		// The rewrite runs whatever the compaction switch (the inspector's
		// schedule loop always runs), so a task created before a restart with
		// compaction off is carried through instead of holding its source
		// frozen in Preparing.
		manager, coordinator, _ := newPreparingCase(t)
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "false")
		manager.compactionEnabled = false
		manager.advanceTasks()
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, mustTask(t, manager, 100).GetState())
		assert.Len(t, coordinator.issued, 1)
	})

	t.Run("a callback that lands during the broadcast is not dragged back", func(t *testing.T) {
		manager, coordinator, _ := newPreparingCase(t)
		svr := &Server{meta: manager.meta, shardSplitTasks: manager.store}
		svr.UpdateStateCode(commonpb.StateCode_Healthy)
		coordinator.onIssue = func(task *datapb.SplitShardTask) {
			status, err := svr.CommitShardSplit(context.Background(), &datapb.CommitShardSplitRequest{
				CollectionId:   splitMgrCollection,
				SplitTaskId:    task.GetTaskId(),
				Sources:        []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV0, SwitchTimeTick: 2000}},
				Targets:        task.GetTargets(),
				RoutingModulus: 2,
			})
			assert.NoError(t, merr.CheckRPCCall(status, err))
		}
		manager.advanceTasks()
		task := mustTask(t, manager, 100)
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
		assert.True(t, task.GetFenced())
		assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
	})
}

func fencedTask(state datapb.SplitShardTaskState) *datapb.SplitShardTask {
	task := preparingTask()
	task.State = state
	task.Fenced = state == datapb.SplitShardTaskState_SplitShardTaskRedistributing
	task.Targets[0].Vchannel = splitMgrV1
	task.Targets[1].Vchannel = splitMgrV2
	if task.Fenced {
		task.Sources[0].SwitchTimeTick = 2000
	}
	return task
}

func TestShardSplitFencingWaitsForTheCallback(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, fencedTask(datapb.SplitShardTaskState_SplitShardTaskFencing)))

	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, mustTask(t, manager, 100).GetState())
	assert.Empty(t, coordinator.issued)

	coordinator.describeErr = merr.WrapErrServiceUnavailableMsg("rootcoord busy")
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, mustTask(t, manager, 100).GetState())

	// The callback ignores a split of a dropped collection, so nothing else
	// would ever end it.
	coordinator.describeErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
	assert.Equal(t, "collection dropped during the write switch", task.GetFailReason())
}

func TestShardSplitRedistributingRunsOnlyPastTheFence(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV1, splitMgrV2}, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV0, schemapb.ShardState_ShardSplitting),
			hashInfo(splitMgrV1, schemapb.ShardState_ShardCreating, 0),
			hashInfo(splitMgrV2, schemapb.ShardState_ShardCreating, 1),
		}, 2), []int64{10}),
	}
	coordinator.drainReason = "source still has a live segment"
	manager, _ := newSplitTestManager(t, coordinator)
	require.NoError(t, manager.store.create(context.Background(), manager.catalog, fencedTask(datapb.SplitShardTaskState_SplitShardTaskRedistributing)))

	// No redistribution wired: a source with data stays in its window.
	manager.advanceTasks()
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, mustTask(t, manager, 100).GetState())

	redistributor := &fakeRedistributor{}
	manager.setRedistributor(redistributor)

	// The source's checkpoint is short of T_switch: nothing moves yet.
	coordinator.fenceReason = "source checkpoint 1999 has not reached its switch time tick 2000"
	manager.advanceTasks()
	assert.Empty(t, redistributor.rounds)

	coordinator.fenceReason = ""
	manager.advanceTasks()
	assert.Equal(t, []int64{100}, redistributor.rounds)

	// A record with no fence -- no callback wrote it -- never moves.
	_, err := manager.store.modify(context.Background(), manager.catalog, 100, func(task *datapb.SplitShardTask) bool {
		task.Sources[0].SwitchTimeTick = 0
		return true
	})
	require.NoError(t, err)
	manager.advanceTasks()
	assert.Equal(t, []int64{100}, redistributor.rounds)

	// A collection dropped mid-redistribution finishes the task.
	coordinator.describeErr = merr.WrapErrCollectionNotFound(splitMgrCollection)
	manager.advanceTasks()
	task := mustTask(t, manager, 100)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskDone, task.GetState())
	assert.Equal(t, "collection dropped during redistribution", task.GetFailReason())
	assert.False(t, splitFenceRecorded(&datapb.SplitShardTask{Fenced: true}))
}

func TestShardSplitManagerTickAndListing(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
	addSplitTestSegment(manager.meta, 1, splitMgrV0, 5000, 1)

	// The first tick detects and advances in one go.
	now := time.Now()
	manager.tick(now)
	task := mustTask(t, manager, manager.store.list()[0].GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, task.GetState())
	assert.Equal(t, now, manager.lastDetect)

	// Within the check interval the trigger does not run again.
	manager.tick(now.Add(time.Second))
	assert.Equal(t, now, manager.lastDetect)

	var listed []metricsinfo.ShardSplitTask
	require.NoError(t, json.Unmarshal([]byte(manager.TaskStatsJSON()), &listed))
	require.Len(t, listed, 1)
	assert.Equal(t, task.GetTaskId(), listed[0].TaskID)
	assert.Equal(t, "SplitShardTaskFencing", listed[0].State)
	assert.Equal(t, splitMgrV0, listed[0].SourceVChannel)
	assert.Equal(t, []string{splitMgrV1, splitMgrV2}, listed[0].TargetVChannels)
	assert.EqualValues(t, 2, listed[0].RoutingModulus)

	stats := splitTaskStats(&datapb.SplitShardTask{
		EndTime: 10, StartTime: 5,
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: splitMgrV0, SwitchTimeTick: 7, PendingSegments: []int64{1, 2}}},
	})
	assert.EqualValues(t, 2, stats.PendingSegments)
	assert.EqualValues(t, 7, stats.SwitchTimeTick)
	assert.NotEmpty(t, stats.EndTime)
	assert.Equal(t, "redistributing", splitTaskStateLabel(datapb.SplitShardTaskState_SplitShardTaskRedistributing))
}

func TestShardSplitManagerStartStop(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitTaskInterval.Key, "0.01")
	defer params.Reset(params.DataCoordCfg.ShardSplitTaskInterval.Key)
	manager, _ := newSplitTestManager(t, &fakeSplitCoordinator{})
	manager.Start()
	time.Sleep(50 * time.Millisecond)
	manager.Stop()
}

func TestShardSplitManagerDefaultWiring(t *testing.T) {
	// The replication role is read off the balancer; one that is not up yet is
	// an error, which the trigger reads as "not primary".
	notReady := mockey.Mock(balance.GetWithContext).Return(nil, errors.New("balancer not ready")).Build()
	_, err := balancerReplicationRole(context.Background())
	assert.Error(t, err)
	notReady.UnPatch()

	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().ReplicateRole().Return(replicateutil.RoleSecondary)
	ready := mockey.Mock(balance.GetWithContext).Return(b, nil).Build()
	role, err := balancerReplicationRole(context.Background())
	ready.UnPatch()
	require.NoError(t, err)
	assert.Equal(t, replicateutil.RoleSecondary, role)

	// The allocator is the streaming node manager's, resolved at call time.
	alloc := mockey.Mock((*snmanager.StreamingNodeManager).AllocVirtualChannels).Return([]string{splitMgrV1}, nil).Build()
	defer alloc.UnPatch()
	vchannels, err := staticVChannelAllocator{}.AllocVirtualChannels(context.Background(), balancer.AllocVChannelParam{CollectionID: 1, Num: 1})
	require.NoError(t, err)
	assert.Equal(t, []string{splitMgrV1}, vchannels)
}

// At most one split per collection is active at a time: a second plan against
// the same collection is made from a record the first split is about to
// change, and would wedge refused before its fence. Concurrency across
// collections is unchanged.
func TestShardSplitTriggerPlansOneSplitPerCollection(t *testing.T) {
	enableShardSplit(t)
	params := paramtable.Get()
	params.Save(params.DataCoordCfg.ShardSplitMaxConcurrentTasks.Key, "2")
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxConcurrentTasks.Key)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0, splitMgrV3}, nil, 0), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0, splitMgrV3)
	addSplitTestSegment(manager.meta, 1, splitMgrV0, 5000, 1)
	addSplitTestSegment(manager.meta, 2, splitMgrV3, 5000, 1)

	manager.detectOnce()
	require.Len(t, manager.store.list(), 1, "two oversized shards of one collection, one split")
	manager.detectOnce()
	require.Len(t, manager.store.list(), 1)

	first := manager.store.list()[0]
	manager.finishTask(first, "")
	manager.detectOnce()
	assert.Len(t, manager.store.list(), 2, "the next split of the collection starts once the first is Done")
}

// A split's targets become shards the trigger must watch the moment rootcoord
// commits them. rootcoord tells datacoord only through BroadcastAlteredCollection,
// so a target that grows past the thresholds is split again -- the cascade --
// only if that broadcast reaches the list the trigger iterates.
func TestShardSplitTriggerSeesTargetsAfterTheAlteredCollectionBroadcast(t *testing.T) {
	enableShardSplit(t)
	postSplit := []string{splitMgrV1, splitMgrV2}
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe(postSplit, []*schemapb.CollectionShardInfo{
			hashInfo(splitMgrV1, schemapb.ShardState_ShardNormal, 0),
			hashInfo(splitMgrV2, schemapb.ShardState_ShardNormal, 1),
		}, 2), []int64{10}),
	}
	manager, _ := newSplitTestManager(t, coordinator)
	// datacoord cached the collection before the split.
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
	// The target V1 is over the thresholds.
	addSplitTestSegment(manager.meta, 1, splitMgrV1, 600, 1)
	addSplitTestSegment(manager.meta, 2, splitMgrV1, 600, 1)
	// Its sibling V2 holds its half, under the thresholds: the doubling
	// relieved the source, so the target may be split again.
	addSplitTestSegment(manager.meta, 3, splitMgrV2, 900, 1)

	server := &Server{meta: manager.meta}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	resp, err := server.BroadcastAlteredCollection(context.Background(), &datapb.AlterCollectionRequest{
		CollectionID: splitMgrCollection,
		Schema:       splitTestSchema(false),
		PartitionIDs: []int64{10},
		VChannels:    postSplit,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	manager.detectOnce()
	tasks := manager.store.list()
	require.Len(t, tasks, 1, "the trigger must consider the split's target after the broadcast")
	assert.Equal(t, splitMgrV1, splitTaskSource(tasks[0]))
	assert.EqualValues(t, 4, tasks[0].GetRoutingModulus(), "a single-residue target doubles the modulus")
}

// A write switch issue still in flight is not issued again, and the task is
// not re-allocated or re-preempted meanwhile; once it returns, its effect is
// what the next tick sees.
func TestShardSplitPreparingDoesNotReissueAWriteSwitchInFlight(t *testing.T) {
	manager, coordinator, vchannels := newPreparingCase(t)
	release := make(chan struct{})
	coordinator.onIssue = func(*datapb.SplitShardTask) { <-release }
	manager.spawn = func(issue func()) { go issue() }
	preempter := &fakePreempter{}
	manager.setCompactionPreempter(preempter)

	manager.advanceTasks()
	require.Eventually(t, func() bool {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		return len(coordinator.issued) == 1
	}, 5*time.Second, 10*time.Millisecond)
	manager.advanceTasks()
	manager.advanceTasks()
	assert.Len(t, vchannels.params, 1)
	assert.Len(t, preempter.channels, 1)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskPreparing, mustTask(t, manager, 100).GetState())

	close(release)
	require.Eventually(t, func() bool {
		return mustTask(t, manager, 100).GetState() == datapb.SplitShardTaskState_SplitShardTaskFencing
	}, 5*time.Second, 10*time.Millisecond)
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	assert.Len(t, coordinator.issued, 1)
}

// A shard whose split cannot get two target vchannels -- the collection
// already sits on every pchannel but one, say -- is not planned: the trigger
// asks the allocator before it writes a record, so it does not persist a task
// that the next tick aborts, again on every check interval, forever.
func TestShardSplitTriggerPersistsNoTaskItCannotAllocate(t *testing.T) {
	enableShardSplit(t)
	coordinator := &fakeSplitCoordinator{
		coll: splitCollectionFromDescribe(splitTestDescribe([]string{splitMgrV0}, nil, 0), []int64{10}),
	}
	manager, vchannels := newSplitTestManager(t, coordinator)
	addSplitTestCollection(manager.meta, splitTestSchema(false), splitMgrV0)
	addSplitTestSegment(manager.meta, 1, splitMgrV0, 1200, 1)
	vchannels.err = errors.New("not enough pchannels to allocate, expected: 2, got: 1")

	for i := 0; i < 3; i++ {
		manager.detectOnce()
		manager.advanceTasks()
	}
	assert.Empty(t, manager.store.list(), "a split that cannot be allocated left a task record")
	require.NotEmpty(t, vchannels.params)
	assert.Equal(t, balancer.AllocVChannelParam{
		CollectionID: splitMgrCollection, Num: 2, ExistingVChannels: []string{splitMgrV0},
	}, vchannels.params[0])

	// Once there is headroom the split is planned.
	vchannels.err = nil
	manager.detectOnce()
	assert.Len(t, manager.store.list(), 1)
}
