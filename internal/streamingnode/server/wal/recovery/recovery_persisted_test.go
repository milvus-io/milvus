package recovery

import (
	"context"
	"os"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestMain(m *testing.M) {
	// Initialize the paramtable package
	paramtable.Init()

	// Run the tests
	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}
}

func TestInitRecoveryInfoFromMeta(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return([]*streamingpb.SegmentAssignmentMeta{}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).Return([]*streamingpb.VChannelMeta{}, nil)

	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))
	channel := types.PChannelInfo{Name: "test_channel"}

	lastConfirmed := message.CreateTestTimeTickSyncMessage(t, 1, 1, rmq.NewRmqID(1))
	rs := newRecoveryStorage(channel, utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId:     rmq.NewRmqID(1).IntoProto(),
		TimeTick:      1,
		RecoveryMagic: utility.RecoveryMagicStreamingInitialized,
	}))

	err := rs.recoverRecoveryInfoFromMeta(context.Background(), channel, lastConfirmed.IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.NoError(t, err)
	assert.NotNil(t, rs.checkpoint)
	assert.Equal(t, utility.RecoveryMagicStreamingInitialized, rs.checkpoint.Magic)
	assert.True(t, rs.checkpoint.MessageID.EQ(rmq.NewRmqID(1)))
}

// TestRecoverRecoveryInfoFromMetaReseedsRetiredVChannels: a vchannel can be
// reloaded from the catalog already SPLITTED and Retired -- the routing
// commit that retired it was persisted before a restart. retiredVChannels
// must be re-seeded from that reload, or the persist gate would never learn
// about a pending retirement whose flusher checkpoint had, in the meantime,
// already passed its fence before this streamingnode ever came back up.
func TestRecoverRecoveryInfoFromMetaReseedsRetiredVChannels(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return([]*streamingpb.SegmentAssignmentMeta{}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).Return([]*streamingpb.VChannelMeta{
		{
			Vchannel:      "v0",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			Retired:       true,
			SplitTimeTick: 2000,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
		{
			// a SPLITTED-but-not-yet-retired vchannel must not be tracked.
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
		{
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}, nil)

	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))
	channel := types.PChannelInfo{Name: "test_channel"}

	lastConfirmed := message.CreateTestTimeTickSyncMessage(t, 1, 1, rmq.NewRmqID(1))
	rs := newRecoveryStorage(channel, utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId:     rmq.NewRmqID(1).IntoProto(),
		TimeTick:      1,
		RecoveryMagic: utility.RecoveryMagicStreamingInitialized,
	}))

	err := rs.recoverRecoveryInfoFromMeta(context.Background(), channel, lastConfirmed.IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.NoError(t, err)
	assert.Len(t, rs.retiredVChannels, 1)
	assert.Contains(t, rs.retiredVChannels, "v0")
}

func TestInitRecoveryInfoFromCoord(t *testing.T) {
	var initialedVChannels map[string]*streamingpb.VChannelMeta
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, channel string) ([]*streamingpb.SegmentAssignmentMeta, error) {
		return []*streamingpb.SegmentAssignmentMeta{}, nil
	})
	snCatalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, channel string) ([]*streamingpb.VChannelMeta, error) {
		return lo.Values(initialedVChannels), nil
	})
	snCatalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string, snapshot *metastore.WALRecoverySnapshot) error {
		assert.Empty(t, snapshot.SegmentAssignments)
		assert.Nil(t, snapshot.SalvageCheckpoint)
		assert.NotNil(t, snapshot.ConsumeCheckpoint)
		initialedVChannels = snapshot.VChannels
		return nil
	})

	fc := syncutil.NewFuture[internaltypes.MixCoordClient]()
	c := mocks.NewMockMixCoordClient(t)
	c.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collections: []*rootcoordpb.CollectionInfoOnPChannel{
			{
				CollectionId: 1,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 1},
					{PartitionId: 2},
				},
				Vchannel: "v1",
				State:    etcdpb.CollectionState_CollectionCreated,
			},
			{
				CollectionId: 2,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 3},
					{PartitionId: 4},
				},
				Vchannel: "v2",
				State:    etcdpb.CollectionState_CollectionCreated,
			},
			{
				CollectionId: 3,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 5},
					{PartitionId: 6},
				},
				Vchannel: "v3",
				State:    etcdpb.CollectionState_CollectionDropping,
			},
		},
	}, nil)
	c.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: req.CollectionID,
			Schema:       &schemapb.CollectionSchema{},
		}, nil
	})
	c.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
		assert.Equal(t, "v3", req.GetChannelName())
		return &datapb.DropVirtualChannelResponse{
			Status: merr.Success(),
		}, nil
	})
	fc.Set(c)

	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog), resource.OptMixCoordClient(fc))
	channel := types.PChannelInfo{Name: "test_channel"}
	lastConfirmed := message.CreateTestTimeTickSyncMessage(t, 1, 1, rmq.NewRmqID(1))
	rs := newRecoveryStorage(channel, nil)

	err := rs.recoverRecoveryInfoFromMeta(context.Background(), channel, lastConfirmed.IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.NoError(t, err)
	assert.NotNil(t, rs.checkpoint)
	assert.Len(t, rs.vchannels, 2)
	assert.Len(t, initialedVChannels, 2)
}
