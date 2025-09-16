package util

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	walTypeDefault = "default"
)

type walEnable struct {
	Rocksmq    bool
	Pulsar     bool
	Kafka      bool
	Woodpecker bool
}

// InitAndSelectWALName init and select wal name.
func InitAndSelectWALName() {
	walName := MustSelectWALName()
	message.RegisterDefaultWALName(walName)
}

// MustSelectWALName select wal name.
func MustSelectWALName() message.WALName {
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
func mustSelectWALName(standalone bool, mqType string, enable walEnable) message.WALName {
	if mqType != walTypeDefault {
		mqName, err := validateWALName(standalone, mqType)
		if err != nil {
			panic(err)
		}
		return mqName
	}
	if standalone {
		if enable.Rocksmq {
			return message.WALNameRocksmq
		}
	}
	if enable.Pulsar {
		return message.WALNamePulsar
	}
	if enable.Kafka {
		return message.WALNameKafka
	}
	if enable.Woodpecker {
		return message.WALNameWoodpecker
	}
	panic(errors.Errorf("no available wal config found, %s, enable: %+v", mqType, enable))
}

// Validate mq type.
func validateWALName(standalone bool, mqType string) (message.WALName, error) {
	mqName := message.NewWALName(mqType)
	if mqName == message.WALNameUnknown || mqName == message.WALNameTest {
		return mqName, errors.Errorf("mq %s is not valid", mqType)
	}

	// we may register more mq type by plugin.
	// so we should not check all mq type here.
	// only check standalone type.
	if !standalone && mqName == message.WALNameRocksmq {
		return mqName, errors.Newf("mq %s is only valid in standalone mode", mqType)
	}
	return mqName, nil
}
