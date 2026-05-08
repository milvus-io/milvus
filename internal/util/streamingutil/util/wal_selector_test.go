package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
