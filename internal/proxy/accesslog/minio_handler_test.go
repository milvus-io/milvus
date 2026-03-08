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

package accesslog

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMinioHandler_ConnectError(t *testing.T) {
	var params paramtable.ComponentParam
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	params.Save(params.MinioCfg.UseIAM.Key, "true")
	params.Save(params.MinioCfg.Address.Key, "")
	params.Save(params.MinioCfg.UseSSL.Key, "true")
	params.Save(params.MinioCfg.SslCACert.Key, "/tmp/dummy.crt")

	_, err := NewMinioHandler(
		context.Background(),
		&params.MinioCfg,
		params.ProxyCfg.AccessLog.RemotePath.GetValue(),
		params.ProxyCfg.AccessLog.MaxBackups.GetAsInt(),
	)
	assert.Error(t, err)
}

func TestMinHandler_Basic(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/miniotest"
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "accesslog")
	Params.Save(Params.ProxyCfg.AccessLog.MaxBackups.Key, "8")
	// close retention
	Params.Save(Params.ProxyCfg.AccessLog.RemoteMaxTime.Key, "0")

	err := os.MkdirAll(testPath, 0o744)
	assert.NoError(t, err)
	defer os.RemoveAll(testPath)

	// init MinioHandler
	handler, err := NewMinioHandler(
		context.Background(),
		&Params.MinioCfg,
		Params.ProxyCfg.AccessLog.RemotePath.GetValue(),
		int(Params.ProxyCfg.AccessLog.MaxBackups.GetAsInt32()),
	)
	assert.NoError(t, err)
	defer handler.Clean()

	prefix, ext := "accesslog", ".log"
	// create a log file to upload
	err = createAndUpdateFile(handler, time.Now(), testPath, prefix, ext)
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	// check if upload success
	lists, err := handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lists))

	// delete file from minio
	err = handler.removeWithPrefix(prefix)
	assert.NoError(t, err)
	time.Sleep(500 * time.Millisecond)

	// check if delete success
	lists, err = handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(lists))
}

func TestMinioHandler_WithTimeRetention(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/miniotest"
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "accesslog")
	Params.Save(Params.ProxyCfg.AccessLog.MaxBackups.Key, "8")
	Params.Save(Params.ProxyCfg.AccessLog.RemoteMaxTime.Key, "168")

	err := os.MkdirAll(testPath, 0o744)
	assert.NoError(t, err)
	defer os.RemoveAll(testPath)

	handler, err := NewMinioHandler(
		context.Background(),
		&Params.MinioCfg,
		Params.ProxyCfg.AccessLog.RemotePath.GetValue(),
		int(Params.ProxyCfg.AccessLog.MaxBackups.GetAsInt32()),
	)
	assert.NoError(t, err)
	defer handler.Clean()

	prefix, ext := "accesslog", ".log"
	handler.retentionPolicy = getTimeRetentionFunc(Params.ProxyCfg.AccessLog.RemoteMaxTime.GetAsInt(), prefix, ext)

	// create a log file
	err = createAndUpdateFile(handler, time.Now(), testPath, prefix, ext)
	assert.NoError(t, err)

	// mock a log file like time interval was large than RemoteMaxTime
	oldTime := time.Now().Add(-1 * time.Duration(Params.ProxyCfg.AccessLog.RemoteMaxTime.GetAsInt()+1) * time.Hour)
	err = createAndUpdateFile(handler, oldTime, testPath, prefix, ext)
	assert.NoError(t, err)

	// create a irrelevant file
	err = createAndUpdateFile(handler, time.Now(), testPath, "irrelevant", ext)
	assert.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	lists, err := handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(lists))

	handler.Retention()
	time.Sleep(500 * time.Millisecond)
	// after retention the old file will be removed
	lists, err = handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lists))
}

func createAndUpdateFile(handler *minioHandler, t time.Time, rootPath, prefix, ext string) error {
	oldFileName := prefix + t.Format(timeNameFormat) + ext
	oldFilePath := path.Join(rootPath, oldFileName)
	oldFileMode := os.FileMode(0o644)
	_, err := os.OpenFile(oldFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, oldFileMode)
	if err != nil {
		return err
	}

	handler.update(oldFilePath, oldFileName)
	return nil
}
