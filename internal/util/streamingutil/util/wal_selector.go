package util

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	walTypeDefault = "default"
	walTypeNatsmq  = "natsmq"
	walTypeRocksmq = "rocksmq"
	walTypeKafka   = "kafka"
	walTypePulsar  = "pulsar"
)

type walEnable struct {
	Rocksmq bool
	Pulsar  bool
	Kafka   bool
}

// MustSelectWALName select wal name.
func MustSelectWALName() string {
	params := paramtable.Get()
	standalone := params.RuntimeConfig.Role.GetAsString() == typeutil.StandaloneRole
	return mustSelectWALName(standalone, params.MQCfg.Type.GetValue(), walEnable{
		params.RocksmqEnable(),
		params.PulsarEnable(),
		params.KafkaEnable(),
	})
}

// mustSelectWALName select wal name.
func mustSelectWALName(standalone bool, mqType string, enable walEnable) string {
	if mqType != walTypeDefault {
		if err := validateWALName(standalone, mqType); err != nil {
			panic(err)
		}
		return mqType
	}
	if standalone {
		if enable.Rocksmq {
			return walTypeRocksmq
		}
	}
	if enable.Pulsar {
		return walTypePulsar
	}
	if enable.Kafka {
		return walTypeKafka
	}
	panic(errors.Errorf("no available wal config found, %s, enable: %+v", mqType, enable))
}

// Validate mq type.
func validateWALName(standalone bool, mqType string) error {
	// we may register more mq type by plugin.
	// so we should not check all mq type here.
	// only check standalone type.
	if !standalone && mqType == walTypeRocksmq {
		return errors.Newf("mq %s is only valid in standalone mode", mqType)
	}
	return nil
}
