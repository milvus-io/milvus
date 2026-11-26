package wp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestEmbeddedLocalStorageRoot(t *testing.T) {
	params := paramtable.Get()
	setParam := func(t *testing.T, item *paramtable.ParamItem, value string) {
		t.Helper()
		old := item.SwapTempValue(value)
		t.Cleanup(func() { item.SwapTempValue(old) })
	}
	setParam(t, &params.WoodpeckerCfg.StorageType, "local")

	t.Run("DefaultRoot", func(t *testing.T) {
		base := t.TempDir()
		setParam(t, &params.LocalStorageCfg.Path, base)
		setParam(t, &params.WoodpeckerCfg.RootPath, "default")
		t.Cleanup(func() { require.NoError(t, woodpecker.StopEmbedLogStore()) })

		wpDir := filepath.Join(base, "wp")
		require.NoDirExists(t, wpDir)
		walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
		require.DirExists(t, wpDir)
	})

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing")
	for _, tc := range []struct{ name, base, root string }{
		{"MissingBase", missing, "default"},
		{"MissingCustomRoot", dir, missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setParam(t, &params.LocalStorageCfg.Path, tc.base)
			setParam(t, &params.WoodpeckerCfg.RootPath, tc.root)
			_, err := (&builderImpl{}).Build()
			require.ErrorIs(t, err, os.ErrNotExist)
			require.ErrorContains(t, err, missing)
			require.NoDirExists(t, missing)
		})
	}
}

func TestWAL(t *testing.T) {
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    t.TempDir(),
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
			storageType: "service", // Using default storage type minio-compatible
			rootPath:    t.TempDir(),
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
				cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, serviceSeeds)
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
