package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateWALType(t *testing.T) {
	assert.Error(t, validateWALName(false, walTypeNatsmq))
	assert.Error(t, validateWALName(false, walTypeRocksmq))
}

func TestSelectWALType(t *testing.T) {
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{true, true, true, true}), walTypeRocksmq)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, true, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, true}), walTypeKafka)
	assert.Panics(t, func() { mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{true, true, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, true, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true}), walTypeKafka)
	assert.Panics(t, func() { mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(true, walTypeRocksmq, walEnable{true, true, true, true}), walTypeRocksmq)
	assert.Equal(t, mustSelectWALName(true, walTypeNatsmq, walEnable{true, true, true, true}), walTypeNatsmq)
	assert.Equal(t, mustSelectWALName(true, walTypePulsar, walEnable{true, true, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(true, walTypeKafka, walEnable{true, true, true, true}), walTypeKafka)
	assert.Panics(t, func() { mustSelectWALName(false, walTypeRocksmq, walEnable{true, true, true, true}) })
	assert.Panics(t, func() { mustSelectWALName(false, walTypeNatsmq, walEnable{true, true, true, true}) })
	assert.Equal(t, mustSelectWALName(false, walTypePulsar, walEnable{true, true, true, true}), walTypePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeKafka, walEnable{true, true, true, true}), walTypeKafka)
}
