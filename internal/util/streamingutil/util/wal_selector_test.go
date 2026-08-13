package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestWoodpeckerLocalStorageInClusterMode(t *testing.T) {
	paramtable.Init()
	storageTypeKey := paramtable.Get().WoodpeckerCfg.StorageType.Key
	forceLocalKey := paramtable.Get().WoodpeckerCfg.ForceLocalStorage.Key

	// save original values and restore after test
	originalStorageType := paramtable.Get().WoodpeckerCfg.StorageType.GetValue()
	originalForceLocal := paramtable.Get().WoodpeckerCfg.ForceLocalStorage.GetValue()
	defer func() {
		paramtable.Get().Save(storageTypeKey, originalStorageType)
		paramtable.Get().Save(forceLocalKey, originalForceLocal)
	}()

	t.Run("cluster_woodpecker_local_should_panic", func(t *testing.T) {
		paramtable.Get().Save(storageTypeKey, "local")
		paramtable.Get().Save(forceLocalKey, "false")
		// auto-select path
		assert.Panics(t, func() {
			mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true})
		})
		// explicit path
		assert.Panics(t, func() {
			mustSelectWALName(false, message.WALNameWoodpecker.String(), walEnable{true, true, true, true})
		})
	})

	t.Run("cluster_woodpecker_local_force_should_pass", func(t *testing.T) {
		paramtable.Get().Save(storageTypeKey, "local")
		paramtable.Get().Save(forceLocalKey, "true")
		// auto-select path
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true}))
		// explicit path
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(false, message.WALNameWoodpecker.String(), walEnable{true, true, true, true}))
	})

	t.Run("cluster_woodpecker_minio_should_pass", func(t *testing.T) {
		paramtable.Get().Save(storageTypeKey, "minio")
		paramtable.Get().Save(forceLocalKey, "false")
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(false, walTypeDefault, walEnable{false, false, false, true}))
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(false, message.WALNameWoodpecker.String(), walEnable{true, true, true, true}))
	})

	t.Run("standalone_woodpecker_local_should_pass", func(t *testing.T) {
		paramtable.Get().Save(storageTypeKey, "local")
		paramtable.Get().Save(forceLocalKey, "false")
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(true, walTypeDefault, walEnable{false, false, false, true}))
		assert.Equal(t, message.WALNameWoodpecker, mustSelectWALName(true, message.WALNameWoodpecker.String(), walEnable{true, true, true, true}))
	})
}

func TestInitAndSelectWALNameDoesNotWriteRuntimeOverlay(t *testing.T) {
	paramtable.Init()

	walName := InitAndSelectWALName()

	assert.NotEqual(t, message.WALNameUnknown, walName)
	assert.Equal(t, walName, message.GetDefaultWALName())

	// mq.type must keep being served from its original config source. Writing the
	// resolved name into the runtime overlay would permanently shadow the etcd
	// source and hide a later WAL switch from every reader (issue #51497).
	mqTypeKey := paramtable.Get().MQCfg.Type.Key
	source, _, err := paramtable.GetBaseTable().Manager().GetConfig(mqTypeKey)
	assert.NoError(t, err)
	assert.NotEqual(t, config.RuntimeSource, source)
}

// The review counterexample for the reserve check: with mq.type "default" the
// selector picks Pulsar (enabled and highest priority in cluster mode), and a
// deployment that raised Pulsar's limit to fit a 16 MiB reserve is perfectly
// valid -- even though that reserve exceeds the untouched 10 MiB defaults of
// kafka.producer.message.max.bytes and woodpecker.maxMessageSize, both of
// which look "enabled" purely because kafka.brokerList and
// woodpecker.meta.prefix have non-empty defaults. Only the backend actually
// selected may be validated; an unused backend's limit must never fail a
// startup. The flip side: when the selected backend's own bound really is
// violated, the once-per-process startup entry fails fast.
func TestInitAndSelectWALNameValidatesOnlySelectedBackend(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	defer params.Reset(params.PulsarCfg.MaxMessageSize.Key)
	defer params.Reset(params.PulsarCfg.MessageReserveSize.Key)

	require.NoError(t, params.Save(params.PulsarCfg.MaxMessageSize.Key, "33554432"))
	require.NoError(t, params.Save(params.PulsarCfg.MessageReserveSize.Key, "16777216"))

	var walName message.WALName
	assert.NotPanics(t, func() { walName = InitAndSelectWALName() })
	assert.Equal(t, message.WALNamePulsar, walName)

	// Same reserve with Pulsar's limit back at its 2 MiB default: now the
	// selected backend's own bound is violated and startup must fail fast.
	require.NoError(t, params.Save(params.PulsarCfg.MaxMessageSize.Key, "2097152"))
	assert.Panics(t, func() { InitAndSelectWALName() })

	// MustSelectWALName is also the per-request WAL name lookup (the proxy
	// resolves it on every Insert/Upsert/Delete), so the same bad combination
	// must NOT panic there: after startup it can only be reached through a
	// live config update, and the write path falls back through
	// normalizePulsarMessageReserve's runtime normalization instead.
	assert.NotPanics(t, func() { walName = MustSelectWALName() })
	assert.Equal(t, message.WALNamePulsar, walName)
}
