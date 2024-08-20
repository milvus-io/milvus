package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	_ "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const vChannel = "by-dev-rootcoord-dml_4"

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestStreamingProduce(t *testing.T) {
	t.Skip()
	streaming.Init()
	defer streaming.Release()
	msg, _ := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{1, 2, 3},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionID: 1,
		}).
		WithVChannel(vChannel).
		BuildMutable()
	resp, err := streaming.WAL().RawAppend(context.Background(), msg)
	fmt.Printf("%+v\t%+v\n", resp, err)

	for i := 0; i < 500; i++ {
		time.Sleep(time.Millisecond * 1)
		msg, _ := message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{
				CollectionId: 1,
			}).
			WithBody(&msgpb.InsertRequest{
				CollectionID: 1,
			}).
			WithVChannel(vChannel).
			BuildMutable()
		resp, err := streaming.WAL().RawAppend(context.Background(), msg)
		fmt.Printf("%+v\t%+v\n", resp, err)
	}

	for i := 0; i < 500; i++ {
		time.Sleep(time.Millisecond * 1)
		txn, err := streaming.WAL().Txn(context.Background(), streaming.TxnOption{
			VChannel:  vChannel,
			Keepalive: 100 * time.Millisecond,
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
				WithVChannel(vChannel).
				BuildMutable()
			err := txn.Append(context.Background(), msg)
			fmt.Printf("%+v\n", err)
		}
		result, err := txn.Commit(context.Background())
		if err != nil {
			t.Errorf("txn failed: %v", err)
		}
		fmt.Printf("%+v\n", result)
	}

	msg, _ = message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			CollectionID: 1,
		}).
		WithVChannel(vChannel).
		BuildMutable()
	resp, err = streaming.WAL().RawAppend(context.Background(), msg)
	fmt.Printf("%+v\t%+v\n", resp, err)
}

func TestStreamingConsume(t *testing.T) {
	t.Skip()
	streaming.Init()
	defer streaming.Release()
	ch := make(message.ChanMessageHandler, 10)
	s := streaming.WAL().Read(context.Background(), streaming.ReadOption{
		VChannel:       vChannel,
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
			fmt.Printf("msgID=%+v, msgType=%+v, tt=%d, lca=%+v, body=%s, idx=%d\n",
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
