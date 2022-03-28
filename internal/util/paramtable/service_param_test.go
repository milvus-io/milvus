// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"io/ioutil"
	"os"
	"path"
	"syscall"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/stretchr/testify/assert"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	SParams.Init()

	t.Run("test etcdConfig", func(t *testing.T) {
		Params := SParams.EtcdCfg

		assert.NotZero(t, len(Params.Endpoints))
		t.Logf("etcd endpoints = %s", Params.Endpoints)

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath)

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath)

		// test UseEmbedEtcd
		Params.Base.Save("etcd.use.embed", "true")
		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode))
		assert.Panics(t, func() { Params.initUseEmbedEtcd() })

		assert.Nil(t, os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode))
		Params.LoadCfgToMemory()
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		Params := SParams.PulsarCfg

		assert.NotEqual(t, Params.Address, "")
		t.Logf("pulsar address = %s", Params.Address)

		assert.Equal(t, Params.MaxMessageSize, SuggestPulsarMaxMessageSize)
	})

	t.Run("test rocksmqConfig", func(t *testing.T) {
		Params := SParams.RocksmqCfg

		assert.NotEqual(t, Params.Path, "")
		t.Logf("rocksmq path = %s", Params.Path)
	})

	t.Run("test minioConfig", func(t *testing.T) {
		Params := SParams.MinioCfg

		addr := Params.Address
		equal := addr == "localhost:9000" || addr == "minio:9000"
		assert.Equal(t, equal, true)
		t.Logf("minio address = %s", Params.Address)

		assert.Equal(t, Params.AccessKeyID, "minioadmin")

		assert.Equal(t, Params.SecretAccessKey, "minioadmin")

		assert.Equal(t, Params.UseSSL, false)

		t.Logf("Minio BucketName = %s", Params.BucketName)

		t.Logf("Minio rootpath = %s", Params.RootPath)
	})

	t.Run("test minioConfig without port", func(t *testing.T) {
		var withoutPortMinioConfig = `
minio:
  address: localhost # Address of MinIO/S3
  port: # empty port
  accessKeyID: minioadmin # accessKeyID of MinIO/S3
  secretAccessKey: minioadmin # MinIO/S3 encryption string
  useSSL: false # Access to MinIO/S3 with SSL
  bucketName: "a-bucket" # Bucket name in MinIO/S3
  rootPath: files # The root path where the message is stored in MinIO/S3
log:
  level: info # Only supports debug, info, warn, error, panic, or fatal. Default 'info'.
  file:
    rootPath: "" # default to stdout, stderr
    maxSize: 300 # MB
    maxAge: 10 # Maximum time for log retention in day.
    maxBackups: 20
  format: text # text/json
`
		var sParams ServiceParam

		tmpDir := os.TempDir()

		syscall.Setenv("MILVUSCONF", tmpDir)
		defer syscall.Unsetenv("MILVUSCONF")

		ioutil.WriteFile(path.Join(tmpDir, "milvus.yaml"), []byte(withoutPortMinioConfig), 0600)

		sParams.BaseTable.Init()
		sParams.MinioCfg.init(&sParams.BaseTable)

		Params := sParams.MinioCfg
		assert.Equal(t, Params.Address, "localhost")

		t.Logf("Minio Address = %s", Params.Address)
	})
}
