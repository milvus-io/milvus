package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

// TestCheckIfMessageFromStreaming tests CheckIfMessageFromStreaming function.
func TestCheckIfMessageFromStreaming(t *testing.T) {
	assert.False(t, CheckIfMessageFromStreaming(nil))
	assert.False(t, CheckIfMessageFromStreaming(map[string]string{}))
	assert.True(t, CheckIfMessageFromStreaming(map[string]string{
		messageVersion: "1",
	}))
}
