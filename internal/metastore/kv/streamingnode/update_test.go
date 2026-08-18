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

package streamingnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// TestCatalog_SaveRecoverySnapshot_Nil proves a nil snapshot issues no KV
// call (mocks.NewMetaKv with no EXPECT: any KV call fails the test).
func TestCatalog_SaveRecoverySnapshot_Nil(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	catalog := NewCataLog(kv)
	assert.NoError(t, catalog.SaveRecoverySnapshot(context.Background(), "p1", nil))
}

// TestCatalog_SaveRecoverySnapshot_Atomic proves a full snapshot that fits
// the etcd txn limit is applied as a single guarded MultiSaveAndRemove call,
// carrying every part's key: segment assignment, vchannel, salvage
// checkpoint, and consume checkpoint.
func TestCatalog_SaveRecoverySnapshot_Atomic(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var saves map[string]string
	var removals []string
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			saves = s
			removals = dels
			return nil
		}).Once()
	catalog := NewCataLog(kv)

	snapshot := &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		},
		VChannels: map[string]*streamingpb.VChannelMeta{
			"vch1": {
				Vchannel:       "vch1",
				State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{},
			},
		},
		SalvageCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "cluster1"},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 42},
	}
	err := catalog.SaveRecoverySnapshot(context.Background(), "p1", snapshot)
	assert.NoError(t, err)

	assert.Empty(t, removals)
	assert.Contains(t, saves, buildSegmentAssignmentKey("p1", 1))
	assert.Contains(t, saves, buildVChannelKey("p1", "vch1"))
	assert.Contains(t, saves, buildSalvageCheckpointPath("p1", "cluster1"))
	assert.Contains(t, saves, buildConsumeCheckpointKey("p1"))
	assert.Len(t, saves, 4)
}

// TestCatalog_SaveRecoverySnapshot_EmptyPartsSkipped proves nil/empty parts
// of the snapshot are skipped: only the parts that are set produce KV ops
// (the initialize-recover-info shape: vchannels + consume checkpoint only).
func TestCatalog_SaveRecoverySnapshot_EmptyPartsSkipped(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var saves map[string]string
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			saves = s
			assert.Empty(t, dels)
			return nil
		}).Once()
	catalog := NewCataLog(kv)

	err := catalog.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			"vch1": {Vchannel: "vch1", CollectionInfo: &streamingpb.CollectionInfoOfVChannel{}},
		},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 1},
	})
	assert.NoError(t, err)
	assert.Len(t, saves, 2)
	assert.Contains(t, saves, buildVChannelKey("p1", "vch1"))
	assert.Contains(t, saves, buildConsumeCheckpointKey("p1"))
}

// TestCatalog_SaveRecoverySnapshot_FlushedSegmentIsRemoved proves a flushed
// segment assignment is staged as a Remove, matching SaveSegmentAssignments'
// encoding.
func TestCatalog_SaveRecoverySnapshot_FlushedSegmentIsRemoved(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var saves map[string]string
	var removals []string
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			saves = s
			removals = dels
			return nil
		}).Once()
	catalog := NewCataLog(kv)

	err := catalog.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED},
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, saves)
	assert.Equal(t, []string{buildSegmentAssignmentKey("p1", 1)}, removals)
}

// TestCatalog_SaveRecoverySnapshot_DroppedVChannelIsRemoved proves a dropped
// vchannel (and its schema versions) is staged as Removes, matching
// SaveVChannels' encoding.
func TestCatalog_SaveRecoverySnapshot_DroppedVChannelIsRemoved(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	var removals []string
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			removals = dels
			return nil
		}).Once()
	catalog := NewCataLog(kv)

	err := catalog.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			"vch1": {
				Vchannel: "vch1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					Schemas: []*streamingpb.CollectionSchemaOfVChannel{{CheckpointTimeTick: 5}},
				},
			},
		},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{
		buildVChannelSchemaKey("p1", "vch1", 5),
		buildVChannelKey("p1", "vch1"),
	}, removals)
}

// TestCatalog_SaveRecoverySnapshot_VChannelEncodingMatchesEncoder proves the
// vchannel part of the composite write produces exactly the kvs the shared
// getRemovalAndSaveForVChannel encoder emits, rather than diverging.
func TestCatalog_SaveRecoverySnapshot_VChannelEncodingMatchesEncoder(t *testing.T) {
	vchannels := map[string]*streamingpb.VChannelMeta{
		"vch1": {
			Vchannel: "vch1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{CheckpointTimeTick: 1, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
				},
			},
		},
	}

	// Expected kvs straight from the shared encoder (no removals for a NORMAL
	// vchannel with a NORMAL schema).
	expectedRemoves, expectedSaves, err := (&catalog{}).getRemovalAndSaveForVChannel("p1", vchannels["vch1"])
	assert.NoError(t, err)
	assert.Empty(t, expectedRemoves)

	var compositeSaves map[string]string
	kv2 := mocks.NewMetaKv(t)
	kv2.EXPECT().MaxTxnOps().Return(128).Maybe()
	kv2.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			compositeSaves = s
			assert.Empty(t, dels)
			return nil
		}).Once()
	catalog2 := NewCataLog(kv2)
	assert.NoError(t, catalog2.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{VChannels: vchannels}))

	assert.Equal(t, expectedSaves, compositeSaves)
}

// TestCatalog_SaveRecoverySnapshot_ConsumeCheckpointLastOnFallback proves
// that, when the snapshot exceeds the store's txn op limit and Commit falls
// back to the ordered chunked path, the consume checkpoint - staged with
// CommitSave - is applied strictly last, after every other part (segment
// assignment, salvage checkpoint) has been flushed.
func TestCatalog_SaveRecoverySnapshot_ConsumeCheckpointLastOnFallback(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	// A limit of 1 forces the chunked fallback path.
	kv.EXPECT().MaxTxnOps().Return(1).Maybe()
	var calls []string
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string]string) error {
		for k := range kvs {
			calls = append(calls, "save:"+k)
		}
		return nil
	}).Twice()
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, s map[string]string, dels []string, _ ...predicates.Predicate) error {
			for k := range s {
				calls = append(calls, "commit:"+k)
			}
			return nil
		}).Once()

	catalog := NewCataLog(kv)
	snapshot := &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		},
		SalvageCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "cluster1"},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 42},
	}
	err := catalog.SaveRecoverySnapshot(context.Background(), "p1", snapshot)
	assert.NoError(t, err)

	assert.Equal(t, []string{
		"save:" + buildSegmentAssignmentKey("p1", 1),
		"save:" + buildSalvageCheckpointPath("p1", "cluster1"),
		"commit:" + buildConsumeCheckpointKey("p1"),
	}, calls)
}
