package wp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    t.TempDir(),
		},
		{
			name:        "ObjectStorage",
			storageType: "minio", // Using default storage type minio-compatible
			rootPath:    "",      // No need to specify path for this storage
		},
	}
	wpBackendTypeKey := paramtable.Get().WoodpeckerCfg.StorageType.Key
	wpBackendRootPathKey := paramtable.Get().WoodpeckerCfg.RootPath.Key
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := paramtable.Get().Save(wpBackendTypeKey, tc.storageType)
			assert.NoError(t, err)
			err = paramtable.Get().Save(wpBackendRootPathKey, tc.rootPath)
			assert.NoError(t, err)
			walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr)
		})
	}
}
