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

package nmq

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var natsServerAddress string

func TestMain(m *testing.M) {
	paramtable.Init()

	storeDir, _ := os.MkdirTemp("", "milvus_mq_nmq")
	defer os.RemoveAll(storeDir)

	cfg := ParseServerOption(paramtable.Get())
	cfg.Opts.Port = server.RANDOM_PORT
	cfg.Opts.StoreDir = storeDir
	MustInitNatsMQ(cfg)
	defer CloseNatsMQ()

	natsServerAddress = Nmq.ClientURL()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestInitNatsMQ(t *testing.T) {
	func() {
		cfg := ParseServerOption(paramtable.Get())
		storeDir, _ := os.MkdirTemp("", "milvus_mq_nmq")
		defer os.RemoveAll(storeDir)
		cfg.Opts.StoreDir = storeDir
		cfg.Opts.Port = server.RANDOM_PORT
		cfg.Opts.LogFile = ""
		mq, err := initNatsMQ(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, mq)
		mq.Shutdown()
		mq.WaitForShutdown()
	}()

	func() {
		cfg := ParseServerOption(paramtable.Get())
		storeDir, _ := os.MkdirTemp("", "milvus_mq_nmq")
		defer os.RemoveAll(storeDir)
		cfg.Opts.StoreDir = storeDir
		cfg.Opts.Port = server.RANDOM_PORT
		cfg.Opts.LogFile = ""
		mq, err := initNatsMQ(cfg)
		assert.NoError(t, err)
		assert.NotNil(t, mq)
		mq.Shutdown()
		mq.WaitForShutdown()
	}()

	func() {
		cfg := ParseServerOption(paramtable.Get())
		storeDir, _ := os.MkdirTemp("", "milvus_mq_nmq")
		defer os.RemoveAll(storeDir)
		cfg.Opts.StoreDir = storeDir
		cfg.Opts.Port = server.RANDOM_PORT
		cfg.Opts.MaxPending = -1
		mq, err := initNatsMQ(cfg)
		assert.Error(t, err)
		assert.Nil(t, mq)
	}()

	func() {
		ex, err := os.Executable()
		assert.NoError(t, err)
		cfg := ParseServerOption(paramtable.Get())
		storeDir, _ := os.MkdirTemp("", "milvus_mq_nmq")
		defer os.RemoveAll(storeDir)
		cfg.Opts.StoreDir = storeDir
		cfg.Opts.Port = server.RANDOM_PORT
		cfg.Opts.LogFile = fmt.Sprintf("%s/test", ex)
		mq, err := initNatsMQ(cfg)
		assert.Error(t, err)
		assert.Nil(t, mq)
	}()
}

func TestGetServerOptionDefault(t *testing.T) {
	cfg := ParseServerOption(paramtable.Get())
	assert.Equal(t, "127.0.0.1", cfg.Opts.Host)
	assert.Equal(t, 4222, cfg.Opts.Port)
	assert.Equal(t, true, cfg.Opts.JetStream)
	assert.Equal(t, "/var/lib/milvus/nats", cfg.Opts.StoreDir)
	assert.Equal(t, int64(17179869184), cfg.Opts.JetStreamMaxStore)
	assert.Equal(t, int32(8388608), cfg.Opts.MaxPayload)
	assert.Equal(t, int64(67108864), cfg.Opts.MaxPending)
	assert.Equal(t, 4000*time.Millisecond, cfg.InitializeTimeout)
	assert.Equal(t, false, cfg.Opts.Debug)
	assert.Equal(t, false, cfg.Opts.Trace)
	assert.Equal(t, true, cfg.Opts.Logtime)
	assert.Equal(t, "/tmp/milvus/logs/nats.log", cfg.Opts.LogFile)
	assert.Equal(t, int64(536870912), cfg.Opts.LogSizeLimit)
}
