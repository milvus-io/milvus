package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var vChannels = []string{
	"by-dev-rootcoord-dml_4",
	"by-dev-rootcoord-dml_5",
}

var collectionName = "test"

func TestMain(m *testing.M) {
	streamingutil.SetStreamingServiceEnabled()
	paramtable.Init()
	m.Run()
}

func TestReplicate(t *testing.T) {
	t.Skip("cat not running without streaming service at background")

	streaming.Init()
	defer streaming.Release()

	pchannels1 := make([]string, 0, len(vChannels))
	pchannels2 := make([]string, 0, len(vChannels))
	for idx := 0; idx < 16; idx++ {
		pchannels1 = append(pchannels1, fmt.Sprintf("primary-rootcoord-dml_%d", idx))
		pchannels2 = append(pchannels2, fmt.Sprintf("by-dev-rootcoord-dml_%d", idx))
	}

	ctx := context.Background()
	err := streaming.WAL().Replicate().UpdateReplicateConfiguration(ctx, &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "primary",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: pchannels1,
			},
			{
				ClusterId: "by-dev",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: pchannels2,
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "primary",
				TargetClusterId: "by-dev",
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

func TestReplicateCreateCollection(t *testing.T) {
	t.Skip("cat not running without streaming service at background")
	streaming.Init()
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}
	schemaBytes, err := proto.Marshal(schema)
	if err != nil {
		panic(err)
	}

	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{2},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionID:   1,
			CollectionName: collectionName,
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
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		_, err := streaming.WAL().Replicate().Append(context.Background(), message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
		if err != nil {
			panic(err)
		}
	}
}

func TestStreamingBroadcast(t *testing.T) {
	t.Skip("cat not running without streaming service at background")
	streamingutil.SetStreamingServiceEnabled()
	streaming.Init()
	defer streaming.Release()

	msg, _ := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{1, 2, 3},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				Timestamp: 1,
			},
			CollectionID:   1,
			CollectionName: collectionName,
		}).
		WithBroadcast(vChannels, message.NewExclusiveCollectionNameResourceKey("db", collectionName)).
		BuildBroadcast()

	resp, err := streaming.WAL().Broadcast().Append(context.Background(), msg)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	t.Logf("CreateCollection: %+v\t%+v\n", resp, err)

	// repeated broadcast with same resource key should be rejected
	resp2, err := streaming.WAL().Broadcast().Append(context.Background(), msg)
	assert.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsResourceAcquired())
	assert.Nil(t, resp2)
}

func TestStreamingProduce(t *testing.T) {
	t.Skip("cat not running without streaming service at background")
	streamingutil.SetStreamingServiceEnabled()
	streaming.Init()
	defer streaming.Release()
	msg, _ := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{1, 2, 3},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreateCollection,
				Timestamp: 1,
			},
			CollectionID:   1,
			CollectionName: collectionName,
		}).
		WithBroadcast(vChannels).
		BuildBroadcast()

	resp, err := streaming.WAL().Broadcast().Append(context.Background(), msg)
	t.Logf("CreateCollection: %+v\t%+v\n", resp, err)

	for i := 0; i < 500; i++ {
		time.Sleep(time.Millisecond * 1)
		msg, _ := message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{
				CollectionId: 1,
			}).
			WithBody(&msgpb.InsertRequest{
				CollectionID: 1,
			}).
			WithVChannel(vChannels[0]).
			BuildMutable()
		resp, err := streaming.WAL().RawAppend(context.Background(), msg)
		t.Logf("Insert: %+v\t%+v\n", resp, err)
	}

	for i := 0; i < 500; i++ {
		time.Sleep(time.Millisecond * 1)
		txn, err := streaming.WAL().Txn(context.Background(), streaming.TxnOption{
			VChannel:  vChannels[0],
			Keepalive: 500 * time.Millisecond,
		})
		if err != nil {
			t.Errorf("txn failed: %v", err)
			return
		}
		for j := 0; j < 5; j++ {
			msg, _ := message.NewInsertMessageBuilderV1().
				WithHeader(&message.InsertMessageHeader{
					CollectionId: 1,
				}).
				WithBody(&msgpb.InsertRequest{
					CollectionID: 1,
				}).
				WithVChannel(vChannels[0]).
				BuildMutable()
			err := txn.Append(context.Background(), msg)
			fmt.Printf("%+v\n", err)
		}
		result, err := txn.Commit(context.Background())
		if err != nil {
			t.Errorf("txn failed: %v", err)
		}
		t.Logf("txn commit: %+v\n", result)
	}

	msg, _ = message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			CollectionID: 1,
		}).
		WithBroadcast(vChannels).
		BuildBroadcast()
	resp, err = streaming.WAL().Broadcast().Append(context.Background(), msg)
	t.Logf("DropCollection: %+v\t%+v\n", resp, err)
}

func TestStreamingConsume(t *testing.T) {
	t.Skip("cat not running without streaming service at background")
	streaming.Init()
	defer streaming.Release()
	ch := make(adaptor.ChanMessageHandler, 10)
	s := streaming.WAL().Read(context.Background(), streaming.ReadOption{
		VChannel:       vChannels[0],
		DeliverPolicy:  options.DeliverPolicyAll(),
		MessageHandler: ch,
	})
	defer func() {
		s.Close()
	}()

	idx := 0
	for {
		time.Sleep(10 * time.Millisecond)
		select {
		case msg := <-ch:
			t.Logf("msgID=%+v, msgType=%+v, tt=%d, lca=%+v, body=%s, idx=%d\n",
				msg.MessageID(),
				msg.MessageType(),
				msg.TimeTick(),
				msg.LastConfirmedMessageID(),
				string(msg.Payload()),
				idx,
			)
		case <-time.After(10 * time.Second):
			return
		}
		idx++
	}
}
