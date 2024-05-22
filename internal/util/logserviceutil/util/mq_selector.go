package util

import (
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/atomic"
)

const (
	mqTypeDefault = "default"
	MQTypeNatsmq  = "natsmq"
	MQTypeRocksmq = "rocksmq"
	MQTypeKafka   = "kafka"
	MQTypePulsar  = "pulsar"
)

var isStandAlone = atomic.NewBool(false)

func EnableStandAlone(standalone bool) {
	isStandAlone.Store(standalone)
}

// Select valid mq if mq type is default.
func MustSelectMQType() string {
	standalone := isStandAlone.Load()

	params := paramtable.Get()
	mqType := params.MQCfg.Type.GetValue()
	if mqType != mqTypeDefault {
		if err := validateMQType(standalone, mqType); err != nil {
			panic(err)
		}
		return mqType
	}
	if standalone {
		if params.RocksmqEnable() {
			return MQTypeRocksmq
		}
	}
	if params.PulsarEnable() {
		return MQTypePulsar
	}
	if params.KafkaEnable() {
		return MQTypeKafka
	}
	panic("no available mq config found")
}

// Validate mq type.
func validateMQType(standalone bool, mqType string) error {
	if mqType != MQTypeNatsmq && mqType != MQTypeRocksmq && mqType != MQTypeKafka && mqType != MQTypePulsar {
		return errors.Newf("mq type %s is invalid", mqType)
	}
	if !standalone && (mqType == MQTypeRocksmq || mqType == MQTypeNatsmq) {
		return errors.Newf("mq %s is only valid in standalone mode")
	}
	return nil
}
