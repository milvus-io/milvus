package wp

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(WALName)
	assert.NotNil(t, registeredB)
	assert.Equal(t, WALName, registeredB.Name())

	id, err := message.UnmarshalMessageID(WALName, newMessageIDOfWoodpecker(1, 2).Marshal())
	assert.NoError(t, err)
	assert.True(t, id.EQ(newMessageIDOfWoodpecker(1, 2)))
}

func TestWAL(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestWpWAL")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "minio", // Using default storage type minio-compatible
			rootPath:    "",      // No need to specify path for this storage
		},
	}
	wpBackendTypeKey := paramtable.Get().WoodpeckerCfg.StorageType.Key
	wpBackendRootPathKey := paramtable.Get().WoodpeckerCfg.RootPath.Key
	logLevelKey := paramtable.Get().LogCfg.Level.Key
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := paramtable.Get().Save(wpBackendTypeKey, tc.storageType)
			assert.NoError(t, err)
			err = paramtable.Get().Save(wpBackendRootPathKey, tc.rootPath)
			assert.NoError(t, err)
			err = paramtable.Get().Save(logLevelKey, "debug")
			assert.NoError(t, err)
			walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr)
		})
	}
}
