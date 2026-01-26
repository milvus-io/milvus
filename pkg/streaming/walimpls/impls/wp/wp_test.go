package wp

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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
	registeredB := registry.MustGetBuilder(message.WALNameWoodpecker)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNameWoodpecker, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{
		WALName: commonpb.WALName(message.WALNameWoodpecker),
		Id:      newMessageIDOfWoodpecker(1, 2).Marshal(),
	})
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
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "minio", // Using default storage type minio-compatible
			rootPath:    "",      // No need to specify path for this storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",             // Using default storage type minio-compatible
			rootPath:    rootPath + "_service", // No need to specify path for this storage
			needCluster: true,
		},
	}
	wpBackendTypeKey := paramtable.Get().WoodpeckerCfg.StorageType.Key
	wpBackendRootPathKey := paramtable.Get().WoodpeckerCfg.RootPath.Key
	wpPoolKey := paramtable.Get().WoodpeckerCfg.QuorumBufferPools.Key
	debugKey := paramtable.Get().LogCfg.Level.Key
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// set params
			err := paramtable.Get().Save(wpBackendTypeKey, tc.storageType)
			assert.NoError(t, err)
			err = paramtable.Get().Save(wpBackendRootPathKey, tc.rootPath)
			assert.NoError(t, err)

			// startup cluster if need
			if tc.needCluster {
				paramtable.Get().Save(debugKey, "debug")
				// get default cfg
				cfg, err := config.NewConfiguration()
				assert.NoError(t, err)
				err = setCustomWpConfig(cfg, &paramtable.Get().WoodpeckerCfg)
				assert.NoError(t, err)
				// setup mini cluster
				const nodeCount = 3
				cluster, cfg, _, serviceSeeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = serviceSeeds
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()
				// set back config using miniCluster seeds
				testPools := []config.QuorumBufferPool{
					{
						Name:  "defaultpool",
						Seeds: serviceSeeds,
					},
				}
				jsonBytes, err := json.Marshal(testPools)
				assert.NoError(t, err)
				jsonStr := string(jsonBytes)
				saveErr := paramtable.Get().Save(wpPoolKey, jsonStr)
				assert.NoError(t, saveErr)
			} else {
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}

			// run test
			walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
		})
	}
}
