package dependency

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestValidateMQType(t *testing.T) {
	assert.Error(t, validateMQType(true, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeNatsmq))
	assert.Error(t, validateMQType(false, mqTypeRocksmq))
	assert.NoError(t, validateMQType(true, mqTypeWoodpecker))
	assert.NoError(t, validateMQType(false, mqTypeWoodpecker))
}

func TestSelectMQType(t *testing.T) {
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{true, true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, false, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{true, true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, false, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(true, mqTypeRocksmq, mqEnable{true, true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypeNatsmq, mqEnable{true, true, true, true, true}), mqTypeNatsmq)
	assert.Equal(t, mustSelectMQType(true, mqTypePulsar, mqEnable{true, true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeKafka, mqEnable{true, true, true, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(true, mqTypeWoodpecker, mqEnable{true, true, true, true, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeRocksmq, mqEnable{true, true, true, true, true}) })
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeNatsmq, mqEnable{true, true, true, true, true}) })
	assert.Equal(t, mustSelectMQType(false, mqTypePulsar, mqEnable{true, true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeKafka, mqEnable{true, true, true, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(false, mqTypeWoodpecker, mqEnable{true, true, true, true, true}), mqTypeWoodpecker)
}

func TestHealthCheck(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().PulsarCfg.WebAddress.Key, "")
	paramtable.Get().Reset(paramtable.Get().PulsarCfg.WebAddress.Key)
	paramtable.Get().Save(paramtable.Get().KafkaCfg.Address.Key, "")
	paramtable.Get().Reset(paramtable.Get().KafkaCfg.Address.Key)

	testCases := []struct {
		mqType string
		health bool
	}{
		{mqTypeNatsmq, true},
		{mqTypeRocksmq, true},
		{mqTypePulsar, false},
		{mqTypeKafka, false},
		{mqTypeWoodpecker, true},
		{"invalidType", false},
	}

	for _, tc := range testCases {
		t.Run(tc.mqType, func(t *testing.T) {
			status := HealthCheck(tc.mqType)
			assert.Equal(t, tc.mqType, status.MqType)
			assert.Equal(t, tc.health, status.Health)
		})
	}
}
