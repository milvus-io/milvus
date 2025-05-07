package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateWALType(t *testing.T) {
	assert.Error(t, validateWALName(false, WALTypeRocksmq))
}

func TestSelectWALType(t *testing.T) {
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{true, true, true, true}), WALTypeRocksmq)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, true, true}), WALTypeKafka)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, true}), WALTypeWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{true, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, true, true}), WALTypeKafka)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true}), WALTypeWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(true, WALTypeRocksmq, walEnable{true, true, true, true}), WALTypeRocksmq)
	assert.Equal(t, mustSelectWALName(true, WALTypePulsar, walEnable{true, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(true, WALTypeKafka, walEnable{true, true, true, true}), WALTypeKafka)
	assert.Equal(t, mustSelectWALName(true, WALTypeWoodpecker, walEnable{true, true, true, true}), WALTypeWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(false, WALTypeRocksmq, walEnable{true, true, true, true}) })
	assert.Equal(t, mustSelectWALName(false, WALTypePulsar, walEnable{true, true, true, true}), WALTypePulsar)
	assert.Equal(t, mustSelectWALName(false, WALTypeKafka, walEnable{true, true, true, true}), WALTypeKafka)
	assert.Equal(t, mustSelectWALName(false, WALTypeWoodpecker, walEnable{true, true, true, true}), WALTypeWoodpecker)
}
