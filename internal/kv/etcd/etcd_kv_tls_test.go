package etcdkv

import (
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

func TestLoadParamAndEnsureNotEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}()
	const testYamlContent = `
etcd:
  tls:
    key: "some.key"
    cert: "other.crt"
    caCert: "blah.crt"
`
	yamlFile := path.Join(tmpDir, "test.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte(testYamlContent), 0600))

	var params paramtable.BaseTable
	params.Init()

	require.NoError(t, params.LoadYaml(yamlFile))
	cacert, err := loadParamAndEnsureNotEmpty(&params, kCfgKeyTlsCACert)
	require.NoError(t, err)
	require.Equal(t, "blah.crt", cacert)
	_, err = loadParamAndEnsureNotEmpty(&params, kCfgKeyTlsBase+".key_not_set")
	require.Error(t, err)
}
