package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mocks/github.com/milvus-io/milvus-proto/go-api/v2/mock_hook"
)

func TestMessageType(t *testing.T) {
	s := MessageTypeUnknown.marshal()
	assert.Equal(t, "0", s)
	typ := unmarshalMessageType("0")
	assert.Equal(t, MessageTypeUnknown, typ)
	assert.False(t, MessageTypeUnknown.Valid())

	typ = unmarshalMessageType("882s9")
	assert.Equal(t, MessageTypeUnknown, typ)

	s = MessageTypeTimeTick.marshal()
	typ = unmarshalMessageType(s)
	assert.Equal(t, MessageTypeTimeTick, typ)
	assert.True(t, MessageTypeTimeTick.Valid())

	assert.True(t, MessageTypeTimeTick.IsSystem())
	assert.True(t, MessageTypeTxn.IsSystem())
	assert.True(t, MessageTypeBeginTxn.IsSystem())
	assert.True(t, MessageTypeCommitTxn.IsSystem())
	assert.True(t, MessageTypeRollbackTxn.IsSystem())
	assert.False(t, MessageTypeImport.IsSystem())
	assert.False(t, MessageTypeInsert.IsSystem())
	assert.False(t, MessageTypeDelete.IsSystem())
	assert.False(t, MessageTypeCreateSegment.IsSystem())
	assert.False(t, MessageTypeFlush.IsSystem())
	assert.False(t, MessageTypeManualFlush.IsSystem())
	assert.False(t, MessageTypeCreateCollection.IsSystem())
	assert.False(t, MessageTypeDropCollection.IsSystem())
	assert.False(t, MessageTypeCreatePartition.IsSystem())
	assert.False(t, MessageTypeDropPartition.IsSystem())
}

func TestVersion(t *testing.T) {
	v := newMessageVersionFromString("")
	assert.Equal(t, VersionOld, v)
	assert.Panics(t, func() {
		newMessageVersionFromString("s1")
	})
	v = newMessageVersionFromString("1")
	assert.Equal(t, VersionV1, v)

	assert.True(t, VersionV1.GT(VersionOld))
	assert.True(t, VersionV2.GT(VersionV1))
	assert.True(t, VersionV1.EQ(VersionV1))
	assert.True(t, VersionV2.EQ(VersionV2))
	assert.True(t, VersionOld.EQ(VersionOld))
}

func TestBroadcast(t *testing.T) {
	msg, err := NewCreateCollectionMessageBuilderV1().
		WithHeader(&CreateCollectionMessageHeader{}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithBroadcast([]string{"v1", "v2"}, NewCollectionNameResourceKey("1"), NewImportJobIDResourceKey(1)).
		BuildBroadcast()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	msg.WithBroadcastID(1)
	msgs := msg.SplitIntoMutableMessage()
	assert.NotNil(t, msgs)
	assert.Len(t, msgs, 2)
	assert.Equal(t, *msgs[1].BroadcastHeader(), *msgs[0].BroadcastHeader())
	assert.Equal(t, uint64(1), msgs[1].BroadcastHeader().BroadcastID)
	assert.Len(t, msgs[0].BroadcastHeader().ResourceKeys, 2)
	assert.ElementsMatch(t, []string{"v1", "v2"}, []string{msgs[0].VChannel(), msgs[1].VChannel()})
}

func TestCiper(t *testing.T) {
	// Not broadcast.
	builder := NewInsertMessageBuilderV1().
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{
			ShardName: "123123",
		}).
		WithVChannel("v1").
		WithCipher(&CipherConfig{
			EzID: 1,
		})
	assert.Panics(t, func() {
		builder.BuildMutable()
	})
	c := mock_hook.NewMockCipher(t)
	e := mock_hook.NewMockEncryptor(t)
	e.EXPECT().Encrypt(mock.Anything).RunAndReturn(func(b []byte) ([]byte, error) {
		return []byte("123" + string(b)), nil
	})
	d := mock_hook.NewMockDecryptor(t)
	d.EXPECT().Decrypt(mock.Anything).RunAndReturn(func(b []byte) ([]byte, error) {
		return b[3:], nil
	})
	c.EXPECT().GetEncryptor(mock.Anything).Return(e, []byte("123"), nil)
	c.EXPECT().GetDecryptor(mock.Anything, mock.Anything).Return(d, nil)
	RegisterCipher(c)

	msg, _ := builder.WithCipher(&CipherConfig{
		EzID: 1,
	}).BuildMutable()

	msg2, err := AsMutableInsertMessageV1(msg)
	assert.NoError(t, err)
	body, err := msg2.Body()
	assert.NoError(t, err)
	assert.Equal(t, body.ShardName, "123123")
	assert.Equal(t, msg2.EstimateSize(), 36)
}

// TestCheckIfMessageFromStreaming tests CheckIfMessageFromStreaming function.
func TestCheckIfMessageFromStreaming(t *testing.T) {
	assert.False(t, CheckIfMessageFromStreaming(nil))
	assert.False(t, CheckIfMessageFromStreaming(map[string]string{}))
	assert.True(t, CheckIfMessageFromStreaming(map[string]string{
		messageVersion: "1",
	}))
}
