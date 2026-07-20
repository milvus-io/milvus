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

// errInvalidWALConfig is the sentinel for invalid/unselectable WAL configuration
// detected at startup. All such failures terminate the process (panic), so this
// stays a cockroachdb error rather than a client-facing merr.
var errInvalidWALConfig = errors.New("invalid wal config")

type walEnable struct {
	Rocksmq    bool
	Pulsar     bool
	Kafka      bool
	Woodpecker bool
}

// InitAndSelectWALName init and select wal name.
// The resolved name is intentionally NOT saved into the runtime config overlay:
// the overlay would take priority over the etcd source forever, hiding a later
// WAL switch (mq.type updated in etcd) from every reader in this process.
// Persisting the resolved name is handled by ProcessImmutableConfigs with a
// renderer at coordinator startup instead.
func InitAndSelectWALName() message.WALName {
	walName := MustSelectWALName()
	message.RegisterDefaultWALName(walName)
	return walName
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
			panic(errors.Wrap(errInvalidWALConfig, "woodpecker with local storage is not supported in cluster mode, set woodpecker.storage.forceLocalStorage to true to override"))
		}
		return message.WALNameWoodpecker
	}
	panic(errors.Wrapf(errInvalidWALConfig, "no available wal config found, %s, enable: %+v", mqType, enable))
}

// Validate mq type.
func validateWALName(standalone bool, mqType string) (message.WALName, error) {
	mqName := message.NewWALName(mqType)
	if mqName == message.WALNameUnknown || mqName == message.WALNameTest {
		return mqName, errors.Wrapf(errInvalidWALConfig, "mq %s is not valid", mqType)
	}

	// we may register more mq type by plugin.
	// so we should not check all mq type here.
	// only check standalone type.
	if !standalone && mqName == message.WALNameRocksmq {
		return mqName, errors.Wrapf(errInvalidWALConfig, "mq %s is only valid in standalone mode", mqType)
	}
	// woodpecker with local storage cannot work in cluster mode,
	// because local storage is not shared across nodes.
	if !standalone && mqName == message.WALNameWoodpecker && paramtable.Get().WoodpeckerCfg.StorageType.GetValue() == "local" && !paramtable.Get().WoodpeckerCfg.ForceLocalStorage.GetAsBool() {
		return mqName, errors.Wrapf(errInvalidWALConfig, "woodpecker with local storage is not supported in cluster mode, set woodpecker.storage.forceLocalStorage to true to override")
	}
	return mqName, nil
}
