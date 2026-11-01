package dependency

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestValidateMQType(t *testing.T) {
	assert.Error(t, validateMQType(true, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeDefault))
	assert.Error(t, validateMQType(false, mqTypeRocksmq))
	assert.NoError(t, validateMQType(true, mqTypeWoodpecker))
	assert.NoError(t, validateMQType(false, mqTypeWoodpecker))
}

func TestSelectMQType(t *testing.T) {
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(true, mqTypeDefault, mqEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeDefault, mqEnable{false, false, false, false}) })
	assert.Equal(t, mustSelectMQType(true, mqTypeRocksmq, mqEnable{true, true, true, true}), mqTypeRocksmq)
	assert.Equal(t, mustSelectMQType(true, mqTypePulsar, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(true, mqTypeKafka, mqEnable{true, true, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(true, mqTypeWoodpecker, mqEnable{true, true, true, true}), mqTypeWoodpecker)
	assert.Panics(t, func() { mustSelectMQType(false, mqTypeRocksmq, mqEnable{true, true, true, true}) })
	assert.Equal(t, mustSelectMQType(false, mqTypePulsar, mqEnable{true, true, true, true}), mqTypePulsar)
	assert.Equal(t, mustSelectMQType(false, mqTypeKafka, mqEnable{true, true, true, true}), mqTypeKafka)
	assert.Equal(t, mustSelectMQType(false, mqTypeWoodpecker, mqEnable{true, true, true, true}), mqTypeWoodpecker)
}

func TestTestRocksmqPath(t *testing.T) {
	p1 := testRocksmqPath()
	p2 := testRocksmqPath()
	assert.NotEqual(t, p1, p2, "each call should return a unique path")
	assert.Contains(t, p1, "rdb_data")
}

func TestNewDefaultFactory(t *testing.T) {
	paramtable.Init()
	f := NewDefaultFactory(true)
	assert.NotNil(t, f)
	assert.True(t, f.standAlone)
}

func TestMockDefaultFactory(t *testing.T) {
	paramtable.Init()
	f := MockDefaultFactory(false, paramtable.Get())
	assert.NotNil(t, f)
	assert.False(t, f.standAlone)
}

func TestHealthCheck(t *testing.T) {
	paramtable.Init()
	testCases := []struct {
		name   string
		mqType string
		health bool
	}{
		{"rocksmq", mqTypeRocksmq, true},
		{"pulsar_unhealthy", mqTypePulsar, false},
		{"pulsar_healthy", mqTypePulsar, true},
		{"kafka_unhealthy", mqTypeKafka, false},
		{"kafka_healthy", mqTypeKafka, true},
		{"woodpecker", mqTypeWoodpecker, true},
		{"invalid", "invalidType", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Probe implementations have their own tests; factory routing must not
			// depend on whether a broker happens to be healthy on this host.
			if tc.mqType == mqTypePulsar || tc.mqType == mqTypeKafka {
				probe := msgstream.PulsarHealthCheck
				if tc.mqType == mqTypeKafka {
					probe = msgstream.KafkaHealthCheck
				}
				patched := mockey.Mock(probe).To(func(status *common.MQClusterStatus) {
					assert.Equal(t, tc.mqType, status.MqType)
					status.Health = tc.health
				}).Build()
				t.Cleanup(func() { patched.UnPatch() })
				defer func() { assert.Equal(t, 1, patched.Times()) }()
			}
			status := HealthCheck(tc.mqType)
			assert.Equal(t, tc.mqType, status.MqType)
			assert.Equal(t, tc.health, status.Health)
		})
	}
}

func TestHealthCheckResolvesDefaultMQType(t *testing.T) {
	paramtable.Init()

	status := HealthCheck(mqTypeDefault)

	// "default" is a placeholder, not a real MQ type: it must be resolved to the
	// actually-enabled MQ before probing, otherwise the status reports an unknown
	// type with health=false forever (issue #51497).
	assert.NotEqual(t, mqTypeDefault, status.MqType)
	params := paramtable.Get()
	expected := mustSelectMQType(paramtable.GetRole() == typeutil.StandaloneRole, mqTypeDefault,
		mqEnable{params.RocksmqEnable(), params.PulsarEnable(), params.KafkaEnable(), params.WoodpeckerEnable()})
	assert.Equal(t, expected, status.MqType)
}
