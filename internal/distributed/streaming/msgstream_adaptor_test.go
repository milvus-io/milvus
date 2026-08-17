package streaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
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
}

// TestDelegatorMessageTypesCarryTheSplitFence pins the whitelist that decides
// what a query delegator's msgstream ever sees.
//
// Filtering is silent by construction: a type left out is dropped at the WAL
// scanner, so it never reaches the dispatcher, the flow graph, or any log. The
// shard-split fence was left out, and the effect was a split whose new shards
// were served by nothing between the fence and querycoord adopting them, with
// the source still answering — from its pre-fence data, at a fully advanced
// tsafe — so nothing anywhere reported a problem.
func TestDelegatorMessageTypesCarryTheSplitFence(t *testing.T) {
	required := []message.MessageType{
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeSchemaChange,
		message.MessageTypeAlterCollection,
		message.MessageTypeManualFlush,
		// A delegator that never learns its vchannel was split cannot front the
		// split's targets, and their data goes unserved for the whole window.
		message.MessageTypeSplitShard,
	}
	for _, mt := range required {
		assert.Contains(t, delegatorMessageTypes, mt,
			"a delegator must observe %s; leaving it out drops the message silently", mt)
	}

	// Every entry must be filterable at all: DeliverFilterMessageType panics on
	// a system message type, which would take the delegator down at seek.
	assert.NotPanics(t, func() {
		options.DeliverFilterMessageType(delegatorMessageTypes...)
	})
}
