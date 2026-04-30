package util

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	mqTypeKey := paramtable.Get().MQCfg.Type.Key
	err := paramtable.Get().Save(mqTypeKey, walName.String())
	if err != nil {
		panic(err)
	}
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
		// woodpecker with local storage cannot work in cluster mode,
		// because local storage is not shared across nodes.
		if !standalone && paramtable.Get().WoodpeckerCfg.StorageType.GetValue() == "local" && !paramtable.Get().WoodpeckerCfg.ForceLocalStorage.GetAsBool() {
			panic(errors.Newf("woodpecker with local storage is not supported in cluster mode, set woodpecker.storage.forceLocalStorage to true to override"))
		}
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
	// woodpecker with local storage cannot work in cluster mode,
	// because local storage is not shared across nodes.
	if !standalone && mqName == message.WALNameWoodpecker && paramtable.Get().WoodpeckerCfg.StorageType.GetValue() == "local" && !paramtable.Get().WoodpeckerCfg.ForceLocalStorage.GetAsBool() {
		return mqName, errors.Newf("woodpecker with local storage is not supported in cluster mode, set woodpecker.storage.forceLocalStorage to true to override")
	}
	return mqName, nil
}
