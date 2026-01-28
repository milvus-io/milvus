package msgdispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/stretchr/testify/assert"
)

type mockConsumeMsg struct {
	collectionID string
}

func (m *mockConsumeMsg) GetPosition() *msgpb.MsgPosition { return nil }
func (m *mockConsumeMsg) SetPosition(*msgpb.MsgPosition)  {}
func (m *mockConsumeMsg) GetSize() int                    { return 0 }
func (m *mockConsumeMsg) GetTimestamp() uint64            { return 0 }
func (m *mockConsumeMsg) GetVChannel() string             { return "" }
func (m *mockConsumeMsg) GetPChannel() string             { return "" }
func (m *mockConsumeMsg) GetMessageID() []byte            { return nil }
func (m *mockConsumeMsg) GetID() int64                    { return 0 }
func (m *mockConsumeMsg) GetCollectionID() string         { return m.collectionID }
func (m *mockConsumeMsg) GetType() commonpb.MsgType       { return commonpb.MsgType_CreateCollection }
func (m *mockConsumeMsg) SetTraceCtx(ctx context.Context) {}

func (m *mockConsumeMsg) Unmarshal(unmarshalDispatcher msgstream.UnmarshalDispatcher) (msgstream.TsMsg, error) {
	// Return a new instance every time
	return &msgstream.CreateCollectionMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx: context.Background(),
		},
		CreateCollectionRequest: &msgpb.CreateCollectionRequest{
			CollectionID: 100, // Dummy
		},
	}, nil
}

func TestGroupMessageDataRaceFix(t *testing.T) {
	d, err := NewDispatcher(context.Background(), newMockFactory(), time.Now().UnixNano(), "mock_pchannel_0",
		nil, common.SubscriptionPositionEarliest, 0, false)
	assert.NoError(t, err)

	// Add two targets that both match collectionID "100"
	// vchannel names must contain "100"
	d.AddTarget(newTarget(&StreamConfig{VChannel: "mock_pchannel_0_100v0"}, false))
	d.AddTarget(newTarget(&StreamConfig{VChannel: "mock_pchannel_0_100v1"}, false))

	pack := &msgstream.ConsumeMsgPack{
		Msgs: []msgstream.ConsumeMsg{
			&mockConsumeMsg{collectionID: "100"},
		},
	}

	targetPacks := d.groupAndParseMsgs(pack, nil)

	// Verify we have 2 target packs
	assert.Len(t, targetPacks, 2)

	// Collect the TsMsg objects
	var msgs []msgstream.TsMsg
	for _, tp := range targetPacks {
		assert.Len(t, tp.Msgs, 1)
		msgs = append(msgs, tp.Msgs[0])
	}

	assert.Len(t, msgs, 2)
	// Check if they are different instances
	assert.NotSame(t, msgs[0], msgs[1], "Messages should be different instances to avoid data race")
}
