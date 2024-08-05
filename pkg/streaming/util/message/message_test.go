package message

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
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
}

func TestBuilder(t *testing.T) {
	m, err := NewBeginTxnMessageBuilderV2().
		WithVChannel("vchannel").
		WithHeader(&BeginTxnMessageHeader{}).
		WithBody(&BeginTxnMessageBody{}).
		BuildMutable()
	fmt.Printf("%v, %v\n", m, err)
	m2 := messagespb.Message{
		Payload:    m.Payload(),
		Properties: m.Properties().ToRawMap(),
	}
	h, err := proto.Marshal(&m2)
	fmt.Printf("%v, %v\n", h, err)

	var m3 messagespb.Message
	err = proto.Unmarshal(h, &m3)
	fmt.Printf("%v, %v\n", h, err)

	m4 := NewMutableMessage(m3.Payload, m3.Properties)
	msg, err := AsMutableBeginTxnMessageV2(m4)
	result, err := msg.Body()
	fmt.Printf("%v, %v\n", result, err)
}
