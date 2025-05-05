package streaming

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
)

func TestDelegatorMsgstreamFactory(t *testing.T) {
	factory := NewDelegatorMsgstreamFactory()

	// Test NewMsgStream
	t.Run("NewMsgStream", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("NewMsgStream should panic but did not")
			}
		}()
		_, _ = factory.NewMsgStream(context.Background())
	})

	// Test NewTtMsgStream
	t.Run("NewTtMsgStream", func(t *testing.T) {
		stream, err := factory.NewTtMsgStream(context.Background())
		if err != nil {
			t.Errorf("NewTtMsgStream returned an error: %v", err)
		}
		if stream == nil {
			t.Errorf("NewTtMsgStream returned nil stream")
		}
	})

	// Test NewMsgStreamDisposer
	t.Run("NewMsgStreamDisposer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("NewMsgStreamDisposer should panic but did not")
			}
		}()
		_ = factory.NewMsgStreamDisposer(context.Background())
	})
}

func TestDelegatorMsgstreamAdaptor(t *testing.T) {
	adaptor := &delegatorMsgstreamAdaptor{}

	// Test Close
	t.Run("Close", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Close should not panic but did")
			}
		}()
		adaptor.Close()
	})

	// Test AsProducer
	t.Run("AsProducer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("AsProducer should panic but did not")
			}
		}()
		adaptor.AsProducer(context.Background(), []string{"channel1"})
	})

	// Test Produce
	t.Run("Produce", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Produce should panic but did not")
			}
		}()
		_ = adaptor.Produce(context.Background(), &msgstream.MsgPack{})
	})

	// Test SetRepackFunc
	t.Run("SetRepackFunc", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("SetRepackFunc should panic but did not")
			}
		}()
		adaptor.SetRepackFunc(nil)
	})

	// Test GetProduceChannels
	t.Run("GetProduceChannels", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("GetProduceChannels should panic but did not")
			}
		}()
		_ = adaptor.GetProduceChannels()
	})

	// Test Broadcast
	t.Run("Broadcast", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Broadcast should panic but did not")
			}
		}()
		_, _ = adaptor.Broadcast(context.Background(), &msgstream.MsgPack{})
	})

	// Test AsConsumer
	t.Run("AsConsumer", func(t *testing.T) {
		err := adaptor.AsConsumer(context.Background(), []string{"channel1"}, "subName", common.SubscriptionPositionUnknown)
		if err != nil {
			t.Errorf("AsConsumer returned an error: %v", err)
		}
	})

	// Test Chan
	t.Run("Chan", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Seek should panic if len(msgPositions) != 1 but did not")
			}
		}()
		adaptor.Chan()
	})

	// Test GetUnmarshalDispatcher
	t.Run("GetUnmarshalDispatcher", func(t *testing.T) {
		dispatcher := adaptor.GetUnmarshalDispatcher()
		if dispatcher == nil {
			t.Errorf("GetUnmarshalDispatcher returned nil")
		}
	})

	// Test Seek
	t.Run("Seek", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Seek should panic if len(msgPositions) != 1 but did not")
			}
		}()
		_ = adaptor.Seek(context.Background(), []*msgstream.MsgPosition{}, true)
	})

	// Test GetLatestMsgID
	t.Run("GetLatestMsgID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("GetLatestMsgID should panic but did not")
			}
		}()
		_, _ = adaptor.GetLatestMsgID("channel1")
	})

	// Test CheckTopicValid
	t.Run("CheckTopicValid", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("CheckTopicValid should panic but did not")
			}
		}()
		_ = adaptor.CheckTopicValid("channel1")
	})

	// Test ForceEnableProduce
	t.Run("ForceEnableProduce", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("ForceEnableProduce should panic but did not")
			}
		}()
		adaptor.ForceEnableProduce(true)
	})
}
