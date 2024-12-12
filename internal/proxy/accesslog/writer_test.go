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
	"bytes"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func getText(size int) []byte {
	text := make([]byte, size)

	for i := 0; i < size; i++ {
		text[i] = byte('-')
	}
	return text
}

func TestRotateWriter_Basic(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "access_log/")
	defer os.RemoveAll(testPath)

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)
	defer logger.handler.Clean()
	defer logger.Close()

	num := 100
	text := getText(num)
	n, err := logger.Write(text)
	assert.Equal(t, num, n)
	assert.NoError(t, err)

	err = logger.Rotate()
	assert.NoError(t, err)

	time.Sleep(time.Duration(1) * time.Second)
	logfiles, err := logger.handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logfiles))
}

func TestRotateWriter_TimeRotate(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "access_log/")
	Params.Save(Params.ProxyCfg.AccessLog.RotatedTime.Key, "2")
	Params.Save(Params.ProxyCfg.AccessLog.MaxBackups.Key, "0")
	defer os.RemoveAll(testPath)

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)
	defer logger.handler.Clean()
	defer logger.Close()

	num := 100
	text := getText(num)
	n, err := logger.Write(text)
	assert.Equal(t, num, n)
	assert.NoError(t, err)

	time.Sleep(time.Duration(4) * time.Second)
	logfiles, err := logger.handler.listAll()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(logfiles), 1)
}

func TestRotateWriter_SizeRotate(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "access_log/")
	Params.Save(Params.ProxyCfg.AccessLog.MaxSize.Key, "1")
	defer os.RemoveAll(testPath)
	fileNum := 5

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	require.NoError(t, err)
	defer logger.handler.Clean()
	defer logger.Close()

	// return error when write text large than file maxsize
	num := 1024 * 1024
	text := getText(num + 1)
	_, err = logger.Write(text)
	assert.Error(t, err)

	for i := 1; i <= fileNum+1; i++ {
		text = getText(num)
		n, err := logger.Write(text)
		assert.Equal(t, num, n)
		assert.NoError(t, err)
	}

	// assert minio files
	time.Sleep(time.Duration(1) * time.Second)
	remoteFiles, err := logger.handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, fileNum, len(remoteFiles))

	// assert local sealed files num
	localFields, err := logger.oldLogFiles()
	assert.NoError(t, err)
	assert.Equal(t, fileNum, len(localFields))

	// old files should in order
	for i := 0; i < fileNum-1; i++ {
		assert.True(t, localFields[i].timestamp.Before(localFields[i+1].timestamp))
	}
}

func TestRotateWriter_LocalRetention(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.MaxBackups.Key, "1")
	defer os.RemoveAll(testPath)

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)
	defer logger.Close()

	// write two sealed files
	logger.Write([]byte("Test"))
	logger.Rotate()
	logger.Write([]byte("Test"))
	logger.Rotate()
	time.Sleep(time.Duration(1) * time.Second)

	logFiles, err := logger.oldLogFiles()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logFiles))
}

func TestRotateWriter_BasicError(t *testing.T) {
	var Params paramtable.ComponentParam
	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := ""
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)
	defer os.RemoveAll(logger.dir())
	defer logger.Close()

	logger.openFileExistingOrNew()

	os.Mkdir(path.Join(logger.dir(), "test"), 0o744)
	logfile, err := logger.oldLogFiles()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(logfile))

	_, err = timeFromName("a.b", "a", "c")
	assert.Error(t, err)
	_, err = timeFromName("a.b", "d", "c")
	assert.Error(t, err)
}

func TestRotateWriter_InitError(t *testing.T) {
	var params paramtable.ComponentParam
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/test"
	params.Save(params.ProxyCfg.AccessLog.Enable.Key, "true")
	params.Save(params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	params.Save(params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	params.Save(params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	params.Save(params.MinioCfg.Address.Key, "")
	// init err with invalid minio address
	_, err := NewRotateWriter(&params.ProxyCfg.AccessLog, &params.MinioCfg)
	assert.Error(t, err)
}

func TestRotateWriter_Close(t *testing.T) {
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.CacheSize.Key, "0")

	logger, err := NewRotateWriter(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)
	defer os.RemoveAll(logger.dir())

	_, err = logger.Write([]byte("test"))
	assert.NoError(t, err)

	logger.Close()

	_, err = logger.Write([]byte("test"))
	assert.Error(t, err)
}

func TestCacheWriter_Normal(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	writer := NewCacheWriter(buffer, 512, 0)

	writer.Write([]byte("111\n"))
	_, err := buffer.ReadByte()
	assert.Error(t, err, io.EOF)

	writer.Flush()
	b, err := buffer.ReadBytes('\n')
	assert.Equal(t, 4, len(b))
	assert.NoError(t, err)

	writer.Write([]byte(strings.Repeat("1", 512) + "\n"))
	b, err = buffer.ReadBytes('\n')
	assert.Equal(t, 513, len(b))
	assert.NoError(t, err)

	writer.Close()
	// writer to closed writer
	_, err = writer.Write([]byte(strings.Repeat("1", 512) + "\n"))
	assert.Error(t, err)
}

type TestWriter struct {
	closed bool
	buffer *bytes.Buffer
	mu     sync.Mutex
}

func (w *TestWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.Write(p)
}

func (w *TestWriter) ReadBytes(delim byte) (line []byte, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.ReadBytes(delim)
}

func (w *TestWriter) ReadByte() (byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.ReadByte()
}

func (w *TestWriter) Close() error {
	w.closed = true
	return nil
}

func TestCacheWriter_WithAutoFlush(t *testing.T) {
	buffer := &TestWriter{buffer: bytes.NewBuffer(make([]byte, 0))}
	writer := NewCacheWriterWithCloser(buffer, buffer, 512, 1*time.Second)
	writer.Write([]byte("111\n"))
	_, err := buffer.ReadByte()
	assert.Error(t, err, io.EOF)

	assert.Eventually(t, func() bool {
		b, err := buffer.ReadBytes('\n')
		if err != nil {
			return false
		}
		assert.Equal(t, 4, len(b))
		return true
	}, 3*time.Second, 1*time.Second)

	writer.Close()
	assert.True(t, buffer.closed)
}
