package dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateMQType(t *testing.T) {
	assert.Error(t, validateMQType(true, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeNatsmq))
	assert.Error(t, validateMQType(false, mqTypeRocksmq))
}

func TestSelectMQType(t *testing.T) {
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, true}), mqTypeKafka)
	assert.Panics(t, func() { mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, true}), mqTypeKafka)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(true, mqTypeRocksmq, mqEnable{true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypeNatsmq, mqEnable{true, true, true, true}), mqTypeNatsmq)
	assert.Equal(t, mustSelectMQType(true, mqTypePulsar, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeKafka, mqEnable{true, true, true, true}), mqTypeKafka)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeRocksmq, mqEnable{true, true, true, true}) })
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeNatsmq, mqEnable{true, true, true, true}) })
	assert.Equal(t, mustSelectMQType(false, mqTypePulsar, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeKafka, mqEnable{true, true, true, true}), mqTypeKafka)
}
