package streamingnode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// newTestEtcdCatalog builds a streamingnode catalog backed by a real etcd KV
// under a uuid-scoped root, for round-trip tests that persist and read back.
func newTestEtcdCatalog(t *testing.T, name string) metastore.StreamingNodeCataLog {
	t.Helper()
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	return NewCataLog(etcdkv.NewEtcdKV(etcdCli, name+"-"+uuid.New().String()+"/meta"))
}

func TestCatalogConsumeCheckpoint(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	v := streamingpb.WALCheckpoint{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Return(string(vs), nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.NotNil(t, checkpoint)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", errors.New("err"))
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Error(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", merr.ErrIoKeyNotFound)
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Nil(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.NoError(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("err"))
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.Error(t, err)
}

// TestCatalogSegmentAssignments round-trips segment assignments through the
// compound SaveRecoverySnapshot: GROWING segments are persisted and listed
// back, and a FLUSHED segment is removed from meta while untouched segments
// (absent from the delta) are left in place.
func TestCatalogSegmentAssignments(t *testing.T) {
	catalog := newTestEtcdCatalog(t, "testCatalogSegmentAssignments")
	ctx := context.Background()

	segments, err := catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, segments, 0)
	assert.NoError(t, err)

	err = catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
			2: {SegmentId: 2, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		},
	})
	assert.NoError(t, err)

	segments, err = catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, segments, 2)
	assert.NoError(t, err)

	// A FLUSHED segment is removed; segment 2 is not in the delta, so it stays.
	err = catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED},
		},
	})
	assert.NoError(t, err)

	segments, err = catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, segments, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), segments[0].GetSegmentId())
}

func TestCatalogVChannelSummaryMetas(t *testing.T) {
	ctx := context.Background()

	t.Run("list_empty", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypeIdempotency)).Return(nil, nil, nil)
		metas, err := catalog.ListVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency)
		assert.NoError(t, err)
		assert.Empty(t, metas)
	})

	t.Run("save_and_list", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)
		meta := &streamingpb.VChannelSummaryMeta{
			Vchannel:                   "v1",
			Pchannel:                   "p1",
			ViewType:                   common.VChannelSummaryViewTypeIdempotency,
			SnapshotCheckpointTimetick: 100,
			EvictedWatermarkTimetick:   10,
			EntryCount:                 3,
		}
		data, err := proto.Marshal(meta)
		assert.NoError(t, err)

		expectedKVs := map[string]string{
			buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1"): string(data),
		}
		kv.EXPECT().MultiSave(mock.Anything, expectedKVs).Return(nil)
		err = catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": meta,
		})
		assert.NoError(t, err)

		kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypeIdempotency)).Return(
			[]string{buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1")},
			[]string{string(data)},
			nil,
		)
		metas, err := catalog.ListVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency)
		assert.NoError(t, err)
		assert.Len(t, metas, 1)
		assert.Equal(t, meta.GetVchannel(), metas[0].GetVchannel())
		assert.Equal(t, meta.GetPchannel(), metas[0].GetPchannel())
		assert.Equal(t, meta.GetViewType(), metas[0].GetViewType())
		assert.Equal(t, meta.GetSnapshotCheckpointTimetick(), metas[0].GetSnapshotCheckpointTimetick())
		assert.Equal(t, meta.GetEvictedWatermarkTimetick(), metas[0].GetEvictedWatermarkTimetick())
		assert.Equal(t, meta.GetEntryCount(), metas[0].GetEntryCount())
	})

	t.Run("save_uses_map_key_when_vchannel_empty", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)
		meta := &streamingpb.VChannelSummaryMeta{ViewType: common.VChannelSummaryViewTypeIdempotency}
		stored := &streamingpb.VChannelSummaryMeta{
			Pchannel: "p1",
			Vchannel: "v1",
			ViewType: common.VChannelSummaryViewTypeIdempotency,
		}
		data, err := proto.Marshal(stored)
		assert.NoError(t, err)

		kv.EXPECT().MultiSave(mock.Anything, map[string]string{
			buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1"): string(data),
		}).Return(nil)
		err = catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": meta,
		})
		assert.NoError(t, err)
	})

	t.Run("save_rejects_dimension_mismatch", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		err := catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": {Pchannel: "p2", Vchannel: "v1", ViewType: common.VChannelSummaryViewTypeIdempotency},
		})
		assert.Error(t, err)

		err = catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": {Pchannel: "p1", Vchannel: "v2", ViewType: common.VChannelSummaryViewTypeIdempotency},
		})
		assert.Error(t, err)

		err = catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": {Pchannel: "p1", Vchannel: "v1", ViewType: common.VChannelSummaryViewTypePrimaryKeyIndex},
		})
		assert.Error(t, err)
	})

	t.Run("view_type_isolated_keys", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)
		meta := &streamingpb.VChannelSummaryMeta{
			Pchannel: "p1",
			Vchannel: "v1",
			ViewType: common.VChannelSummaryViewTypePrimaryKeyIndex,
		}
		data, err := proto.Marshal(meta)
		assert.NoError(t, err)

		kv.EXPECT().MultiSave(mock.Anything, map[string]string{
			buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypePrimaryKeyIndex, "v1"): string(data),
		}).Return(nil)
		err = catalog.SaveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypePrimaryKeyIndex, map[string]*streamingpb.VChannelSummaryMeta{
			"v1": meta,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypeIdempotency), buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypePrimaryKeyIndex))
	})

	t.Run("remove", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().MultiRemove(mock.Anything, []string{
			buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1"),
			buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v2"),
		}).Return(nil)
		err := catalog.RemoveVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency, []string{"v1", "v2"})
		assert.NoError(t, err)
	})

	t.Run("list_unmarshal_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypeIdempotency)).Return(
			[]string{buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1")},
			[]string{"invalid-proto"},
			nil,
		)
		metas, err := catalog.ListVChannelSummaryMetas(ctx, "p1", common.VChannelSummaryViewTypeIdempotency)
		assert.Error(t, err)
		assert.Nil(t, metas)
	})
}

// TestCatalogVChannelSummaryMetasRecover pins ListVChannelSummaryMetas against a
// REAL etcdkv, which returns keys that INCLUDE the metaKV rootPath. The prefix
// must be stripped rootPath-tolerantly: a naive strings.TrimPrefix leaves the
// whole key as the vchannel name, recovery fails with "vchannel mismatch", and
// the WAL reopen wedges on streamingnode restart — every collection on the
// pchannel becomes unloadable. The mock-based save_and_list case above cannot
// cover this: the mock returns the relative key, not the rootPath-prefixed one.
func TestCatalogVChannelSummaryMetasRecover(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogVChannelSummaryMetasRecover-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	pchannel := "by-dev-rootcoord-dml_0"
	vchannel := "by-dev-rootcoord-dml_0_123456789v0"
	err := catalog.SaveVChannelSummaryMetas(ctx, pchannel, common.VChannelSummaryViewTypeIdempotency,
		map[string]*streamingpb.VChannelSummaryMeta{
			vchannel: {
				Pchannel:                   pchannel,
				Vchannel:                   vchannel,
				ViewType:                   common.VChannelSummaryViewTypeIdempotency,
				SnapshotCheckpointTimetick: 100,
			},
		})
	assert.NoError(t, err)

	metas, err := catalog.ListVChannelSummaryMetas(ctx, pchannel, common.VChannelSummaryViewTypeIdempotency)
	if assert.NoError(t, err) && assert.Len(t, metas, 1) {
		assert.Equal(t, vchannel, metas[0].GetVchannel())
		assert.Equal(t, pchannel, metas[0].GetPchannel())
		assert.Equal(t, common.VChannelSummaryViewTypeIdempotency, metas[0].GetViewType())
	}

	err = catalog.RemoveVChannelSummaryMetas(ctx, pchannel, common.VChannelSummaryViewTypeIdempotency, []string{vchannel})
	assert.NoError(t, err)
}

func TestCatalogPChannelSummaryMeta(t *testing.T) {
	ctx := context.Background()

	t.Run("get_missing", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().Load(mock.Anything, buildPChannelSummaryMetaKey("p1")).Return("", merr.ErrIoKeyNotFound)
		meta, err := catalog.GetPChannelSummaryMeta(ctx, "p1")
		assert.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("save_and_get", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)
		meta := &streamingpb.PChannelSummaryMeta{
			Pchannel:                 "p1",
			SourceCheckpointTimetick: 120,
			LatestGeneration:         3,
			MinAvailableGeneration:   1,
			SourceCheckpointMessageId: &commonpb.MessageID{
				WALName: commonpb.WALName_Test,
				Id:      "120",
			},
			MinInUseGeneration: 2,
		}
		data, err := proto.Marshal(meta)
		assert.NoError(t, err)

		kv.EXPECT().Save(mock.Anything, buildPChannelSummaryMetaKey("p1"), string(data)).Return(nil)
		err = catalog.SavePChannelSummaryMeta(ctx, "p1", meta)
		assert.NoError(t, err)

		kv.EXPECT().Load(mock.Anything, buildPChannelSummaryMetaKey("p1")).Return(string(data), nil)
		got, err := catalog.GetPChannelSummaryMeta(ctx, "p1")
		assert.NoError(t, err)
		assert.Equal(t, meta.GetPchannel(), got.GetPchannel())
		assert.Equal(t, meta.GetSourceCheckpointTimetick(), got.GetSourceCheckpointTimetick())
		assert.Equal(t, meta.GetMinAvailableGeneration(), got.GetMinAvailableGeneration())
		assert.Equal(t, meta.GetLatestGeneration(), got.GetLatestGeneration())
		assert.Equal(t, meta.GetSourceCheckpointMessageId().GetId(), got.GetSourceCheckpointMessageId().GetId())
		assert.Equal(t, meta.GetMinInUseGeneration(), got.GetMinInUseGeneration())
	})

	t.Run("save_rejects_pchannel_mismatch", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		err := catalog.SavePChannelSummaryMeta(ctx, "p1", &streamingpb.PChannelSummaryMeta{Pchannel: "p2"})
		assert.Error(t, err)
	})

	t.Run("get_unmarshal_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().Load(mock.Anything, buildPChannelSummaryMetaKey("p1")).Return("invalid-proto", nil)
		meta, err := catalog.GetPChannelSummaryMeta(ctx, "p1")
		assert.Error(t, err)
		assert.Nil(t, meta)
	})

	t.Run("compare_and_swap", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv).(*catalog)
		key := buildPChannelSummaryMetaKey("p1")
		current := &streamingpb.PChannelSummaryMeta{
			Pchannel:                 "p1",
			SourceCheckpointTimetick: 100,
			LatestGeneration:         1,
		}
		currentData, err := proto.Marshal(current)
		assert.NoError(t, err)
		target := &streamingpb.PChannelSummaryMeta{
			Pchannel:                 "p1",
			SourceCheckpointTimetick: 200,
			LatestGeneration:         2,
		}
		targetData, err := proto.Marshal(target)
		assert.NoError(t, err)

		kv.EXPECT().CompareVersionAndSwap(mock.Anything, key, int64(0), string(currentData)).Return(true, nil).Once()
		swapped, err := catalog.CompareAndSwapPChannelSummaryMeta(ctx, "p1", nil, current)
		assert.NoError(t, err)
		assert.True(t, swapped)

		kv.EXPECT().CompareVersionAndSwap(mock.Anything, key, int64(0), string(currentData)).Return(false, nil).Once()
		swapped, err = catalog.CompareAndSwapPChannelSummaryMeta(ctx, "p1", nil, current)
		assert.NoError(t, err)
		assert.False(t, swapped)

		kv.EXPECT().MultiSaveAndRemove(mock.Anything, map[string]string{key: string(targetData)}, mock.Anything, mock.Anything).Return(nil).Once()
		swapped, err = catalog.CompareAndSwapPChannelSummaryMeta(ctx, "p1", current, target)
		assert.NoError(t, err)
		assert.True(t, swapped)

		kv.EXPECT().MultiSaveAndRemove(mock.Anything, map[string]string{key: string(targetData)}, mock.Anything, mock.Anything).Return(merr.WrapErrIoFailedReason("failed to execute transaction")).Once()
		swapped, err = catalog.CompareAndSwapPChannelSummaryMeta(ctx, "p1", current, target)
		assert.NoError(t, err)
		assert.False(t, swapped)
	})
}

func TestCatalogVChannel(t *testing.T) {
	catalog := newTestEtcdCatalog(t, "testCatalogVChannel")
	ctx := context.Background()

	channel1 := "p1"
	vchannels, err := catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 0)
	assert.NoError(t, err)

	vchannelMetas := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 100,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-1",
						},
						CheckpointTimeTick: 0,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
					},
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-2",
						},
						CheckpointTimeTick: 8,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-3",
						},
						CheckpointTimeTick: 101,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
		"vchannel-2": {
			Vchannel: "vchannel-2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId: 100,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema: &schemapb.CollectionSchema{
							Name: "collection-1",
						},
						CheckpointTimeTick: 0,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
	}

	err = catalog.SaveRecoverySnapshot(ctx, channel1, &metastore.WALRecoverySnapshot{VChannels: vchannelMetas})
	assert.NoError(t, err)

	vchannels, err = catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 2)
	assert.NoError(t, err)
	for _, vchannel := range vchannels {
		switch vchannel.Vchannel {
		case "vchannel-1":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 2)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-2")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(8))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].Schema.Name, "collection-3")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].CheckpointTimeTick, uint64(101))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		case "vchannel-2":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		}
	}

	vchannelMetas["vchannel-1"].CollectionInfo.Schemas[1].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
	vchannelMetas["vchannel-2"].State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	err = catalog.SaveRecoverySnapshot(ctx, channel1, &metastore.WALRecoverySnapshot{VChannels: vchannelMetas})
	assert.NoError(t, err)

	vchannels, err = catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 1)
	assert.NoError(t, err)
	for _, vchannel := range vchannels {
		switch vchannel.Vchannel {
		case "vchannel-1":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-3")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(101))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		}
	}
}

func TestCatalogSalvageCheckpoint(t *testing.T) {
	ctx := context.Background()

	t.Run("get_success", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		cp := &commonpb.ReplicateCheckpoint{
			ClusterId: "source-cluster",
			Pchannel:  "source-cluster-rootcoord-dml_0",
		}
		cpBytes, err := proto.Marshal(cp)
		assert.NoError(t, err)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"streamingnode-meta/wal/p1/salvage-checkpoint/source-cluster"},
			[]string{string(cpBytes)},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Len(t, checkpoints, 1)
		assert.Equal(t, "source-cluster", checkpoints[0].ClusterId)
		assert.Equal(t, "source-cluster-rootcoord-dml_0", checkpoints[0].Pchannel)
	})

	t.Run("get_load_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, errors.New("etcd error"))
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.Error(t, err)
		assert.Nil(t, checkpoints)
	})

	t.Run("get_unmarshal_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"key"},
			[]string{"invalid-proto-bytes"},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.Error(t, err)
		assert.Nil(t, checkpoints)
	})

	t.Run("get_empty", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(nil, nil, nil)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Empty(t, checkpoints)
	})

	t.Run("get_multiple_clusters", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		cp1 := &commonpb.ReplicateCheckpoint{ClusterId: "cluster-a"}
		cp2 := &commonpb.ReplicateCheckpoint{ClusterId: "cluster-b"}
		bytes1, _ := proto.Marshal(cp1)
		bytes2, _ := proto.Marshal(cp2)

		kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return(
			[]string{"key1", "key2"},
			[]string{string(bytes1), string(bytes2)},
			nil,
		)
		checkpoints, err := catalog.GetSalvageCheckpoint(ctx, "p1")
		assert.NoError(t, err)
		assert.Len(t, checkpoints, 2)
	})
}

// TestCatalogSaveRecoverySnapshotRoundTrip persists a full recovery snapshot -
// segment assignments, vchannels, salvage checkpoint and the consume
// checkpoint (the commit marker) - in one compound write, then reads every
// part back through its own accessor. This is the end-to-end replacement for
// the removed per-category SaveSegmentAssignments / SaveVChannels /
// SaveSalvageCheckpoint writers.
func TestCatalogSaveRecoverySnapshotRoundTrip(t *testing.T) {
	catalog := newTestEtcdCatalog(t, "testCatalogSnapshot")
	ctx := context.Background()
	pchannel := "p1"

	err := catalog.SaveRecoverySnapshot(ctx, pchannel, &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		},
		VChannels: map[string]*streamingpb.VChannelMeta{
			"vchannel-1": {
				Vchannel: "vchannel-1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 100,
					Schemas: []*streamingpb.CollectionSchemaOfVChannel{
						{
							Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
							CheckpointTimeTick: 1,
							State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						},
					},
				},
			},
		},
		SalvageCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "cluster-a", Pchannel: "p1-rootcoord-dml_0"},
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: 42},
	})
	assert.NoError(t, err)

	segments, err := catalog.ListSegmentAssignment(ctx, pchannel)
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, int64(1), segments[0].GetSegmentId())

	vchannels, err := catalog.ListVChannel(ctx, pchannel)
	assert.NoError(t, err)
	assert.Len(t, vchannels, 1)
	assert.Equal(t, "vchannel-1", vchannels[0].GetVchannel())
	assert.Len(t, vchannels[0].GetCollectionInfo().GetSchemas(), 1)

	salvage, err := catalog.GetSalvageCheckpoint(ctx, pchannel)
	assert.NoError(t, err)
	assert.Len(t, salvage, 1)
	assert.Equal(t, "cluster-a", salvage[0].GetClusterId())

	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, pchannel)
	assert.NoError(t, err)
	assert.NotNil(t, checkpoint)
	assert.Equal(t, uint64(42), checkpoint.GetTimeTick())
}

func TestBuildPrefixAndKey(t *testing.T) {
	// Prefix functions
	assert.Equal(t, "streamingnode-meta/wal/p1/", buildWALPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/", buildWALPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/", buildSegmentAssignmentPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/", buildSegmentAssignmentPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/", buildVChannelPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/", buildVChannelPrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/salvage-checkpoint/", buildSalvageCheckpointPrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/salvage-checkpoint/", buildSalvageCheckpointPrefix("p2"))

	// Key functions
	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/1", buildSegmentAssignmentKey("p1", 1))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/2", buildSegmentAssignmentKey("p2", 2))

	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/v1", buildVChannelKey("p1", "v1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/v2", buildVChannelKey("p2", "v2"))
	assert.Equal(t, "streamingnode-meta/wal/p1/vchannel/v1/schema/100", buildVChannelSchemaKey("p1", "v1", 100))
	assert.Equal(t, "streamingnode-meta/wal/p2/vchannel/v2/schema/200", buildVChannelSchemaKey("p2", "v2", 200))

	assert.Equal(t, "streamingnode-meta/wal/p1/consume-checkpoint", buildConsumeCheckpointKey("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/consume-checkpoint", buildConsumeCheckpointKey("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/", buildSummaryStorePrefix("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/summary-store/", buildSummaryStorePrefix("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/pchannel-summary-meta", buildPChannelSummaryMetaKey("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/summary-store/pchannel-summary-meta", buildPChannelSummaryMetaKey("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/vchannel-summary-meta/idempotency/", buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypeIdempotency))
	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/vchannel-summary-meta/pkindex/", buildVChannelSummaryMetaPrefix("p1", common.VChannelSummaryViewTypePrimaryKeyIndex))
	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/vchannel-summary-meta/idempotency/v1", buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypeIdempotency, "v1"))
	assert.Equal(t, "streamingnode-meta/wal/p1/summary-store/vchannel-summary-meta/pkindex/v1", buildVChannelSummaryMetaKey("p1", common.VChannelSummaryViewTypePrimaryKeyIndex, "v1"))

	assert.Equal(t, "streamingnode-meta/wal/p1/salvage-checkpoint/cluster-a", buildSalvageCheckpointPath("p1", "cluster-a"))
	assert.Equal(t, "streamingnode-meta/wal/p2/salvage-checkpoint/cluster-b", buildSalvageCheckpointPath("p2", "cluster-b"))
}
