package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestValidateWALType(t *testing.T) {
	_, err := validateWALName(false, message.WALNameRocksmq.String())
	assert.Error(t, err)
}

func TestSelectWALType(t *testing.T) {
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{true, true, true, true}), message.WALNameRocksmq)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, true, true}), message.WALNameKafka)
	assert.Equal(t, mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, true}), message.WALNameWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{true, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, true, true}), message.WALNameKafka)
	assert.Equal(t, mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true}), message.WALNameWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectWALName(true, message.WALNameRocksmq.String(), walEnable{true, true, true, true}), message.WALNameRocksmq)
	assert.Equal(t, mustSelectWALName(true, message.WALNamePulsar.String(), walEnable{true, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(true, message.WALNameKafka.String(), walEnable{true, true, true, true}), message.WALNameKafka)
	assert.Equal(t, mustSelectWALName(true, message.WALNameWoodpecker.String(), walEnable{true, true, true, true}), message.WALNameWoodpecker)
	assert.Panics(t, func() { mustSelectWALName(false, message.WALNameRocksmq.String(), walEnable{true, true, true, true}) })
	assert.Equal(t, mustSelectWALName(false, message.WALNamePulsar.String(), walEnable{true, true, true, true}), message.WALNamePulsar)
	assert.Equal(t, mustSelectWALName(false, message.WALNameKafka.String(), walEnable{true, true, true, true}), message.WALNameKafka)
	assert.Equal(t, mustSelectWALName(false, message.WALNameWoodpecker.String(), walEnable{true, true, true, true}), message.WALNameWoodpecker)
}
