package streamingnode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

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

func TestCatalogSegmentAssignments(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	k := "p1"
	v := streamingpb.SegmentAssignmentMeta{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return([]string{k}, []string{string(vs)}, nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	metas, err := catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, metas, 1)
	assert.NoError(t, err)

	kv.EXPECT().MultiRemove(mock.Anything, mock.Anything).Return(nil)
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil)

	err = catalog.SaveSegmentAssignments(ctx, "p1", map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {
			SegmentId: 1,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		},
		2: {
			SegmentId: 2,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		},
	})
	assert.NoError(t, err)
}

func TestCatalogVChannel(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	rootPath := "testCatalogVChannel-" + uuid.New().String() + "/meta"
	kv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	catalog := NewCataLog(kv)
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

	err = catalog.SaveVChannels(ctx, channel1, vchannelMetas)
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
	err = catalog.SaveVChannels(ctx, channel1, vchannelMetas)
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

	t.Run("save_and_get_success", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		cp := &commonpb.ReplicateCheckpoint{
			ClusterId: "source-cluster",
			Pchannel:  "source-cluster-rootcoord-dml_0",
		}
		cpBytes, err := proto.Marshal(cp)
		assert.NoError(t, err)

		kv.EXPECT().Save(mock.Anything, mock.Anything, string(cpBytes)).Return(nil)
		err = catalog.SaveSalvageCheckpoint(ctx, "p1", cp)
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

	t.Run("save_error", func(t *testing.T) {
		kv := mocks.NewMetaKv(t)
		catalog := NewCataLog(kv)

		kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("etcd error"))
		err := catalog.SaveSalvageCheckpoint(ctx, "p1", &commonpb.ReplicateCheckpoint{ClusterId: "c1"})
		assert.Error(t, err)
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
}
