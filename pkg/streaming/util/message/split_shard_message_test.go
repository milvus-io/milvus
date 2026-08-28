package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestSplitShardMessage(t *testing.T) {
	header := &message.SplitShardMessageHeader{
		CollectionId:   100,
		SplitTaskId:    1,
		RoutingModulus: 2,
		Targets: []*message.SplitShardTarget{
			{
				Vchannel: "by-dev-rootcoord-dml_0_100v1",
				Routing:  &schemapb.HashRouting{Buckets: []uint64{0}},
			},
			{
				Vchannel: "by-dev-rootcoord-dml_1_100v2",
				Routing:  &schemapb.HashRouting{Buckets: []uint64{1}},
			},
		},
	}
	m, err := message.NewSplitShardMessageBuilderV2().
		WithVChannel("by-dev-rootcoord-dml_0_100v0").
		WithHeader(header).
		WithBody(&message.SplitShardMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	assert.Equal(t, message.MessageTypeSplitShard, m.MessageType())
	assert.Equal(t, "by-dev-rootcoord-dml_0_100v0", m.VChannel())

	splitMsg, err := message.AsMutableSplitShardMessageV2(m)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), splitMsg.Header().CollectionId)
	assert.Equal(t, int64(1), splitMsg.Header().SplitTaskId)
	assert.Len(t, splitMsg.Header().Targets, 2)
	assert.Equal(t, "by-dev-rootcoord-dml_0_100v1", splitMsg.Header().Targets[0].Vchannel)
	assert.Equal(t, []uint64{0}, splitMsg.Header().Targets[0].Routing.Buckets)
	assert.Equal(t, []uint64{1}, splitMsg.Header().Targets[1].Routing.Buckets)
	assert.EqualValues(t, 2, splitMsg.Header().RoutingModulus)

	// converting to a mismatched specialized type should fail.
	_, err = message.AsMutableManualFlushMessageV2(m)
	assert.Error(t, err)

	id := mock_message.NewMockMessageID(t)
	id.EXPECT().String().Return("1").Maybe()
	im := m.IntoImmutableMessage(id)
	immutableMsg, err := message.AsImmutableSplitShardMessageV2(im)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), immutableMsg.Header().CollectionId)
	assert.Len(t, immutableMsg.Header().Targets, 2)
	body, err := immutableMsg.Body()
	assert.NoError(t, err)
	assert.NotNil(t, body)
}

func TestSplitShardMessageTypeProperties(t *testing.T) {
	typ := message.MessageTypeSplitShard
	assert.True(t, typ.Valid())
	assert.Equal(t, "SplitShard", typ.String())
	// SplitShard is the write fence of the source vchannel: it must be
	// appended exclusively so that all active txns are force-failed and
	// no DML can append concurrently.
	assert.True(t, typ.IsExclusiveRequired())
	assert.False(t, typ.IsSystem())
	assert.False(t, typ.IsSelfControlled())
	assert.False(t, typ.IsDMLMessageType())
	assert.False(t, typ.CanEnableCipher())
	assert.Equal(t, mlog.InfoLevel, typ.LogLevel())
}
