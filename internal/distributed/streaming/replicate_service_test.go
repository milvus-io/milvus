package streaming

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestReplicateService(t *testing.T) {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()

	h := mock_handler.NewMockHandlerClient(t)
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		msg := message.MustAsMutableCreateCollectionMessageV1(mm)
		assert.True(t, strings.HasPrefix(msg.VChannel(), "by-dev"))
		for _, vchannel := range msg.BroadcastHeader().VChannels {
			assert.True(t, strings.HasPrefix(vchannel, "by-dev"))
		}
		b := msg.MustBody()
		for _, vchannel := range b.VirtualChannelNames {
			assert.True(t, strings.HasPrefix(vchannel, "by-dev"))
		}
		for _, pchannel := range b.PhysicalChannelNames {
			assert.True(t, strings.HasPrefix(pchannel, "by-dev"))
		}
		return &types.AppendResult{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		}, nil
	}).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
		"by-dev",
		&commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	), nil)
	rs := &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
	replicateMsgs := createReplicateCreateCollectionMessages()

	for _, msg := range replicateMsgs {
		_, err := rs.Append(context.Background(), msg)
		assert.NoError(t, err)
	}
}

func createReplicateCreateCollectionMessages() []message.ReplicateMutableMessage {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}
	schemaBytes, _ := proto.Marshal(schema)
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{2},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionID:   1,
			CollectionName: "collection",
			PartitionName:  "partition",
			PhysicalChannelNames: []string{
				"primary-rootcoord-dml_0",
				"primary-rootcoord-dml_1",
			},
			VirtualChannelNames: []string{
				"primary-rootcoord-dml_0_1v0",
				"primary-rootcoord-dml_1_1v1",
			},
			Schema: schemaBytes,
		}).
		WithBroadcast([]string{"primary-rootcoord-dml_0_1v0", "primary-rootcoord-dml_1_1v1"}).
		MustBuildBroadcast()
	msgs := msg.WithBroadcastID(100).SplitIntoMutableMessage()
	replicateMsgs := make([]message.ReplicateMutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		replicateMsgs = append(replicateMsgs, message.NewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
	}
	return replicateMsgs
}
