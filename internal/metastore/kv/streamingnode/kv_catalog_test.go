package streamingnode

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
}

func TestCatalogQueryViews(t *testing.T) {
	catalog := newTestEtcdCatalog(t, "testCatalogQueryViews")
	ctx := context.Background()
	view := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    10,
			Vchannel:     "p1_1v0",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 20},
				QueryVersion: 30,
			},
			State: viewpb.QueryViewState_QueryViewStateUp,
		},
		QueryNode: []*viewpb.QueryViewOfQueryNode{{
			NodeId: 100,
			Partitions: []*viewpb.QueryViewOfPartition{{
				PartitionId:     200,
				SegmentIds:      []int64{300},
				ReadySegmentIds: []int64{300},
			}},
		}},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	require.NoError(t, catalog.SaveQueryViews(ctx, "p1", []*viewpb.QueryViewOfShard{view}))
	views, err := catalog.ListQueryViews(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Empty(t, views[0].GetQueryNode()[0].GetPartitions()[0].GetReadySegmentIds())

	next := proto.Clone(view).(*viewpb.QueryViewOfShard)
	next.Meta.Version.QueryVersion++
	require.NoError(t, catalog.SaveQueryViews(ctx, "p1", []*viewpb.QueryViewOfShard{next}))
	views, err = catalog.ListQueryViews(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, views, 2)

	view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
	require.NoError(t, catalog.SaveQueryViews(ctx, "p1", []*viewpb.QueryViewOfShard{view}))
	views, err = catalog.ListQueryViews(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, int64(31), views[0].GetMeta().GetVersion().GetQueryVersion())
}

func TestCatalogQueryViewWritesUseReliableMetaKV(t *testing.T) {
	metaKV := &etcdkv.EmbedEtcdKV{}
	var attempts atomic.Int32
	mockWrite := mockey.Mock((*etcdkv.EmbedEtcdKV).MultiSaveAndRemove).
		To(func(_ *etcdkv.EmbedEtcdKV, _ context.Context, _ map[string]string, _ []string, _ ...predicates.Predicate) error {
			if attempts.Add(1) == 1 {
				return merr.WrapErrServiceUnavailableMsg("injected metastore failure")
			}
			return nil
		}).Build()
	t.Cleanup(func() { mockWrite.UnPatch() })

	view := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    10,
			Vchannel:     "p1_1v0",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 20},
				QueryVersion: 30,
			},
			State: viewpb.QueryViewState_QueryViewStateUp,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	require.NoError(t, NewCataLog(metaKV).SaveQueryViews(context.Background(), "p1", []*viewpb.QueryViewOfShard{view}))
	assert.Equal(t, int32(2), attempts.Load())
}

func TestBuildQueryViewKeyRejectsMismatchedIdentity(t *testing.T) {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1,
		ReplicaId:    10,
		Vchannel:     "p1_1v0",
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 20},
			QueryVersion: 30,
		},
	}

	key, err := buildQueryViewKey("p1", meta)
	require.NoError(t, err)
	assert.Equal(t, "streamingnode-meta/wal/p1/qv/1/10/0/20/0/30", key)

	_, err = buildQueryViewKey("p2", meta)
	require.Error(t, err)
	assert.ErrorContains(t, err, "pchannel")

	meta.CollectionId = 2
	_, err = buildQueryViewKey("p1", meta)
	require.Error(t, err)
	assert.ErrorContains(t, err, "collection")
}

func TestCatalogListQueryViewsRejectsCompactKeyValueMismatch(t *testing.T) {
	view := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 1,
			ReplicaId:    10,
			Vchannel:     "p1_1v0",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 20},
				QueryVersion: 30,
			},
			State: viewpb.QueryViewState_QueryViewStateUp,
		},
	}
	value, err := marshalQueryViewForPersistence(view)
	require.NoError(t, err)

	kv := mocks.NewMetaKv(t)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildQueryViewPrefix("p1")).Return(
		[]string{"streamingnode-meta/wal/p1/qv/1/10/1/20/0/30"},
		[]string{string(value)},
		nil,
	)

	views, err := NewCataLog(kv).ListQueryViews(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, views)
	assert.ErrorContains(t, err, "mismatched query view")
}

func TestCatalogConsumeCheckpointControlFields(t *testing.T) {
	ctx := context.Background()

	t.Run("round_trip", func(t *testing.T) {
		catalog := newTestEtcdCatalog(t, "testCatalogCheckpointControl")
		checkpoint, err := catalog.GetConsumeCheckpoint(ctx, "p1")
		require.NoError(t, err)
		require.Nil(t, checkpoint)

		expected := &streamingpb.WALCheckpoint{
			TimeTick: 42,
			ReplicateConfig: &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{{ClusterId: "cluster-a"}},
			},
			AlterWalState: &streamingpb.AlterWALState{Stage: streamingpb.AlterWALStage_FLUSHING},
		}
		require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
			ConsumeCheckpoint: expected,
		}))

		checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
		require.NoError(t, err)
		require.True(t, proto.Equal(expected, checkpoint))
	})
}

// TestCatalogSegmentAssignments round-trips segment assignments through the
// compound SaveRecoverySnapshot. Closed segment metadata stays persisted until
// an explicit cleanup task drops it, while entries absent from the delta remain
// unchanged.
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

	// A FLUSHED segment is retained; segment 2 is not in the delta, so it stays.
	err = catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		SegmentAssignments: map[int64]*streamingpb.SegmentAssignmentMeta{
			1: {SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED},
		},
	})
	assert.NoError(t, err)

	segments, err = catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, segments, 2)
	assert.NoError(t, err)
	byID := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segments))
	for _, segment := range segments {
		byID[segment.GetSegmentId()] = segment
	}
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, byID[1].GetState())
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, byID[2].GetState())
}

func TestCatalogSaveRecoverySnapshotBaseMetaDoesNotRewriteSchemas(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	meta := &streamingpb.VChannelMeta{
		Vchannel: "p1_100v2",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				CheckpointTimeTick: 10,
			}},
		},
	}
	key := buildVChannelKey("p1", meta.GetVchannel())
	kv.EXPECT().MaxTxnOps().Return(128).Maybe()
	kv.EXPECT().MultiSaveAndRemove(mock.Anything, mock.MatchedBy(func(kvs map[string]string) bool {
		require.Len(t, kvs, 1)
		saved, ok := kvs[key]
		if !ok {
			return false
		}
		loaded := &streamingpb.VChannelMeta{}
		return proto.Unmarshal([]byte(saved), loaded) == nil &&
			len(loaded.GetCollectionInfo().GetSchemas()) == 0 &&
			loaded.GetVchannel() == meta.GetVchannel()
	}), mock.Anything).Return(nil)

	catalog := NewCataLog(kv)
	require.NoError(t, catalog.SaveRecoverySnapshot(context.Background(), "p1", &metastore.WALRecoverySnapshot{
		VChannelBaseMetas: map[string]*streamingpb.VChannelMeta{meta.GetVchannel(): meta},
	}))
}

func TestCatalogListSegmentAssignmentRejectsMismatchedOwner(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	segment := &streamingpb.SegmentAssignmentMeta{
		SegmentId: 20,
		State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
	}
	value, err := proto.Marshal(segment)
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildSegmentAssignmentPrefix("p1")).Return(
		[]string{buildSegmentAssignmentKey("p1", 10)},
		[]string{string(value)},
		nil,
	)

	catalog := NewCataLog(kv)
	segments, err := catalog.ListSegmentAssignment(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, segments)
	assert.ErrorContains(t, err, "mismatched segment assignment")
}

func TestCatalogListRecoveryMetaWithRootPath(t *testing.T) {
	kv := newRootedMemoryKV("by-dev/meta")
	catalog := NewCataLog(kv)
	ctx := context.Background()

	segment := &streamingpb.SegmentAssignmentMeta{SegmentId: 10}
	segmentValue, err := proto.Marshal(segment)
	require.NoError(t, err)
	require.NoError(t, kv.Save(ctx, buildSegmentAssignmentKey("p1", 10), string(segmentValue)))

	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "p1_1v0",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				CheckpointTimeTick: 1,
			}},
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{vchannel.GetVchannel(): vchannel},
	}))

	segments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, segments, 1)
	assert.Equal(t, int64(10), segments[0].GetSegmentId())

	vchannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, vchannels, 1)
	assert.Equal(t, vchannel.GetVchannel(), vchannels[0].GetVchannel())
}

func TestCatalogRetainsClosedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogRetainsClosedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 200}},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{VChannels: vchannels}))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, loadedVChannels[0].GetState())
	assert.Equal(t, uint64(100), loadedVChannels[0].GetCheckpointTimeTick())

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          300,
			Vchannel:           "vchannel-1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 120,
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{SegmentAssignments: segments}))

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED, loadedSegments[0].GetState())
	assert.Equal(t, uint64(120), loadedSegments[0].GetCheckpointTimeTick())
}

func TestCatalogListVChannelRejectsMissingSchema(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "v1",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
		},
	}
	value, err := proto.Marshal(vchannel)
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelPrefix("p1")).
		Return([]string{buildVChannelKey("p1", "v1")}, []string{string(value)}, nil)

	catalog := NewCataLog(kv)
	vchannels, err := catalog.ListVChannel(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, vchannels)
	assert.ErrorContains(t, err, "missing schemas")
}

func TestCatalogListVChannelRejectsMismatchedOwner(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "other",
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
		},
	}
	vchannelValue, err := proto.Marshal(vchannel)
	require.NoError(t, err)
	schemaValue, err := proto.Marshal(&streamingpb.CollectionSchemaOfVChannel{
		Schema:             &schemapb.CollectionSchema{Name: "schema"},
		CheckpointTimeTick: 10,
	})
	require.NoError(t, err)
	kv.EXPECT().LoadWithPrefix(mock.Anything, buildVChannelPrefix("p1")).Return(
		[]string{
			buildVChannelKey("p1", "v1"),
			buildVChannelSchemaKey("p1", "v1", 10),
		},
		[]string{string(vchannelValue), string(schemaValue)},
		nil,
	)

	catalog := NewCataLog(kv)
	vchannels, err := catalog.ListVChannel(context.Background(), "p1")
	require.Error(t, err)
	assert.Nil(t, vchannels)
	assert.ErrorContains(t, err, "mismatched vchannel")
}

func TestCatalogRetainsTombstonedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogRetainsTombstonedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Partitions: []*streamingpb.PartitionInfoOfVChannel{
					{
						PartitionId:        200,
						State:              streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED,
						CheckpointTimeTick: 120,
					},
				},
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{VChannels: vchannels}))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, loadedVChannels[0].GetState())
	assert.Equal(t, uint64(100), loadedVChannels[0].GetCheckpointTimeTick())
	require.Len(t, loadedVChannels[0].GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, loadedVChannels[0].GetCollectionInfo().GetPartitions()[0].GetState())
	assert.Equal(t, uint64(120), loadedVChannels[0].GetCollectionInfo().GetPartitions()[0].GetCheckpointTimeTick())

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			CollectionId:       100,
			PartitionId:        200,
			SegmentId:          300,
			Vchannel:           "vchannel-1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick: 120,
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{SegmentAssignments: segments}))

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED, loadedSegments[0].GetState())
	assert.Equal(t, uint64(120), loadedSegments[0].GetCheckpointTimeTick())
}

func TestCatalogDropsTombstonedRecoveryMeta(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogDropsTombstonedRecoveryMeta-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
	ctx := context.Background()

	vchannels := map[string]*streamingpb.VChannelMeta{
		"vchannel-1": {
			Vchannel: "vchannel-1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 100,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
						CheckpointTimeTick: 10,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-2"},
						CheckpointTimeTick: 20,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
			CheckpointTimeTick: 100,
		},
		"vchannel-2": {
			Vchannel: "vchannel-2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 101,
				Schemas: []*streamingpb.CollectionSchemaOfVChannel{
					{
						Schema:             &schemapb.CollectionSchema{Name: "collection-3"},
						CheckpointTimeTick: 30,
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					},
				},
			},
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{VChannels: vchannels}))

	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		300: {
			SegmentId:          300,
			Vchannel:           "vchannel-1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
			CheckpointTimeTick: 120,
		},
		301: {
			SegmentId: 301,
			Vchannel:  "vchannel-2",
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	}
	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{SegmentAssignments: segments}))

	require.NoError(t, catalog.SaveRecoverySnapshot(ctx, "p1", &metastore.WALRecoverySnapshot{
		RemovedVChannels:  vchannelsByName(vchannels, "vchannel-1"),
		RemovedSegmentIDs: []int64{300},
	}))

	loadedVChannels, err := catalog.ListVChannel(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedVChannels, 1)
	assert.Equal(t, "vchannel-2", loadedVChannels[0].GetVchannel())

	loadedSegments, err := catalog.ListSegmentAssignment(ctx, "p1")
	require.NoError(t, err)
	require.Len(t, loadedSegments, 1)
	assert.Equal(t, int64(301), loadedSegments[0].GetSegmentId())
}

func vchannelsByName(vchannels map[string]*streamingpb.VChannelMeta, names ...string) map[string]*streamingpb.VChannelMeta {
	selected := make(map[string]*streamingpb.VChannelMeta, len(names))
	for _, name := range names {
		selected[name] = vchannels[name]
	}
	return selected
}

func TestCatalogRejectsDroppedVChannelSchemaOnSave(t *testing.T) {
	catalog := &catalog{}
	vchannel := &streamingpb.VChannelMeta{
		Vchannel: "vchannel-1",
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "collection-1"},
					CheckpointTimeTick: 10,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
				},
			},
		},
	}

	removes, kvs, err := catalog.getRemovalAndSaveForVChannel("p1", vchannel)
	require.Error(t, err)
	assert.Nil(t, removes)
	assert.Nil(t, kvs)
	assert.ErrorContains(t, err, "unknown vchannel schema state")
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
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
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
			assert.Len(t, vchannel.CollectionInfo.Schemas, 3)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].Schema.Name, "collection-2")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].CheckpointTimeTick, uint64(8))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[1].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].Schema.Name, "collection-3")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].CheckpointTimeTick, uint64(101))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[2].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		case "vchannel-2":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].State, streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL)
		}
	}

	vchannelMetas["vchannel-2"].State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	err = catalog.SaveRecoverySnapshot(ctx, channel1, &metastore.WALRecoverySnapshot{VChannels: vchannelMetas})
	assert.NoError(t, err)

	vchannels, err = catalog.ListVChannel(ctx, channel1)
	assert.Len(t, vchannels, 2)
	assert.NoError(t, err)
	for _, vchannel := range vchannels {
		switch vchannel.Vchannel {
		case "vchannel-1":
			assert.Len(t, vchannel.CollectionInfo.Schemas, 3)
		case "vchannel-2":
			assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, vchannel.GetState())
			assert.Len(t, vchannel.CollectionInfo.Schemas, 1)
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].Schema.Name, "collection-1")
			assert.Equal(t, vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick, uint64(0))
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
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{
			TimeTick: 42,
			// The pchannel control state advances atomically with the checkpoint.
			AlterWalState: &streamingpb.AlterWALState{Stage: streamingpb.AlterWALStage_FLUSHING},
		},
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
	assert.Equal(t, streamingpb.AlterWALStage_FLUSHING, checkpoint.GetAlterWalState().GetStage())
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

	assert.Equal(t, "streamingnode-meta/wal/p1/salvage-checkpoint/cluster-a", buildSalvageCheckpointPath("p1", "cluster-a"))
	assert.Equal(t, "streamingnode-meta/wal/p2/salvage-checkpoint/cluster-b", buildSalvageCheckpointPath("p2", "cluster-b"))
	assert.Equal(t, "streamingnode-meta/wal/p1/qv/", buildQueryViewPrefix("p1"))

}

type rootedMemoryKV struct {
	*memkv.MemoryKV
	rootPath string
}

func newRootedMemoryKV(rootPath string) *rootedMemoryKV {
	return &rootedMemoryKV{
		MemoryKV: memkv.NewMemoryKV(),
		rootPath: rootPath,
	}
}

func (kv *rootedMemoryKV) GetPath(key string) string {
	return kv.rootPath + "/" + key
}

func (kv *rootedMemoryKV) LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error) {
	return kv.MemoryKV.LoadWithPrefix(ctx, kv.GetPath(key))
}

func (kv *rootedMemoryKV) Save(ctx context.Context, key, value string) error {
	return kv.MemoryKV.Save(ctx, kv.GetPath(key), value)
}

func (kv *rootedMemoryKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	rooted := make(map[string]string, len(kvs))
	for key, value := range kvs {
		rooted[kv.GetPath(key)] = value
	}
	return kv.MemoryKV.MultiSave(ctx, rooted)
}

func (kv *rootedMemoryKV) MultiSaveAndRemove(
	ctx context.Context,
	saves map[string]string,
	removals []string,
	preds ...predicates.Predicate,
) error {
	rootedSaves := make(map[string]string, len(saves))
	for key, value := range saves {
		rootedSaves[kv.GetPath(key)] = value
	}
	rootedRemovals := make([]string, 0, len(removals))
	for _, key := range removals {
		rootedRemovals = append(rootedRemovals, kv.GetPath(key))
	}
	return kv.MemoryKV.MultiSaveAndRemove(ctx, rootedSaves, rootedRemovals, preds...)
}

func (kv *rootedMemoryKV) CompareVersionAndSwap(context.Context, string, int64, string) (bool, error) {
	panic("unused")
}

func (kv *rootedMemoryKV) WalkWithPrefix(context.Context, string, int, func([]byte, []byte) error) error {
	panic("unused")
}
