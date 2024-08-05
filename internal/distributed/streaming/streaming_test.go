package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	_ "github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const vChannel = "by-dev-rootcoord-dml_4"

func TestMain(m *testing.M) {
	paramtable.Init()
	etcd, _ := kvfactory.GetEtcdAndPath()
	streaming.Init(etcd)
	defer streaming.Release()
	m.Run()
}

func TestStreamingProduce(t *testing.T) {
	t.Skip()
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
	resp := streaming.WAL().Append(context.Background(), msg)
	fmt.Printf("%+v\n", resp)

	for i := 0; i < 1000; i++ {
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
		resp := streaming.WAL().Append(context.Background(), msg)
		fmt.Printf("%+v\n", resp)
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
	resp = streaming.WAL().Append(context.Background(), msg)
	fmt.Printf("%+v\n", resp)
}

func TestStreamingConsume(t *testing.T) {
	t.Skip()
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
			fmt.Printf("msgID=%+v, tt=%d, lca=%+v, body=%s, idx=%d\n",
				msg.MessageID(),
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
