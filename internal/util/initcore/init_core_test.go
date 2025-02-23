// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initcore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestTracer(t *testing.T) {
	paramtable.Init()
	InitTraceConfig(paramtable.Get())

	paramtable.Get().Save(paramtable.Get().TraceCfg.Exporter.Key, "stdout")
	ResetTraceConfig(paramtable.Get())
}

func TestOtlpHang(t *testing.T) {
	paramtable.Init()
	InitTraceConfig(paramtable.Get())

	paramtable.Get().Save(paramtable.Get().TraceCfg.Exporter.Key, "otlp")
	paramtable.Get().Save(paramtable.Get().TraceCfg.InitTimeoutSeconds.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().TraceCfg.Exporter.Key)
	defer paramtable.Get().Reset(paramtable.Get().TraceCfg.InitTimeoutSeconds.Key)

	assert.NotPanics(t, func() {
		ResetTraceConfig(paramtable.Get())
	})
}

func TestInitStorageV2FileSystem(t *testing.T) {
	// init local storage
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, "/tmp")
	err := InitStorageV2FileSystem(paramtable.Get())
	assert.NoError(t, err)

	// init remote storage
	paramtable.Get().Save(paramtable.Get().MinioCfg.Address.Key, "oss-cn-hangzhou.aliyuncs.com")
	paramtable.Get().Save(paramtable.Get().MinioCfg.BucketName.Key, "test-oss-0815")
	paramtable.Get().Save(paramtable.Get().MinioCfg.AccessKeyID.Key, "test")
	paramtable.Get().Save(paramtable.Get().MinioCfg.SecretAccessKey.Key, "test")
	paramtable.Get().Save(paramtable.Get().MinioCfg.RootPath.Key, "test")
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "remote")
	paramtable.Get().Save(paramtable.Get().MinioCfg.CloudProvider.Key, "aliyun")
	paramtable.Get().Save(paramtable.Get().MinioCfg.IAMEndpoint.Key, "")
	paramtable.Get().Save(paramtable.Get().MinioCfg.LogLevel.Key, "warn")
	paramtable.Get().Save(paramtable.Get().MinioCfg.Region.Key, "oss-cn-hangzhou")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseSSL.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.SslCACert.Key, "")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseIAM.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.UseVirtualHost.Key, "false")
	paramtable.Get().Save(paramtable.Get().MinioCfg.RequestTimeoutMs.Key, "10000")

	err = InitStorageV2FileSystem(paramtable.Get())
	assert.NoError(t, err)
}
