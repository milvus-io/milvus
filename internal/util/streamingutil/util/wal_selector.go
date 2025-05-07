package util

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	walTypeDefault    = "default"
	WALTypeRocksmq    = "rocksmq"
	WALTypeKafka      = "kafka"
	WALTypePulsar     = "pulsar"
	WALTypeWoodpecker = "woodpecker"
)

type walEnable struct {
	Rocksmq    bool
	Pulsar     bool
	Kafka      bool
	Woodpecker bool
}

// MustSelectWALName select wal name.
func MustSelectWALName() string {
	standalone := paramtable.GetRole() == typeutil.StandaloneRole
	params := paramtable.Get()
	return mustSelectWALName(standalone, params.MQCfg.Type.GetValue(), walEnable{
		params.RocksmqEnable(),
		params.PulsarEnable(),
		params.KafkaEnable(),
		params.WoodpeckerEnable(),
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
			return WALTypeRocksmq
		}
	}
	if enable.Pulsar {
		return WALTypePulsar
	}
	if enable.Kafka {
		return WALTypeKafka
	}
	if enable.Woodpecker {
		return WALTypeWoodpecker
	}
	panic(errors.Errorf("no available wal config found, %s, enable: %+v", mqType, enable))
}

// Validate mq type.
func validateWALName(standalone bool, mqType string) error {
	// we may register more mq type by plugin.
	// so we should not check all mq type here.
	// only check standalone type.
	if !standalone && mqType == WALTypeRocksmq {
		return errors.Newf("mq %s is only valid in standalone mode", mqType)
	}
	return nil
}
