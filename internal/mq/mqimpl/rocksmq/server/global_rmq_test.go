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

package server

import (
	"os"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/allocator"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/etcd"
)

func Test_InitRmq(t *testing.T) {
	name := "/tmp/rmq_init"
	defer os.RemoveAll("/tmp/rmq_init")
	err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	assert.NoError(t, err)
	param := new(paramtable.ServiceParam)
	param.Init()
	param.BaseTable.Save("etcd.use.embed", "true")
	param.BaseTable.Save("etcd.config.path", "../../../../../configs/advanced/etcd.yaml")
	param.BaseTable.Save("etcd.data.dir", "etcd.test.data.dir")
	param.EtcdCfg.LoadCfgToMemory()
	//clean up data
	defer func() {
		os.RemoveAll("etcd.test.data.dir")
	}()
	etcdCli := etcd.GetEtcdTestClient(t)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/etcd/test/root")
	idAllocator := allocator.NewGlobalIDAllocator("dummy", etcdKV)
	_ = idAllocator.Initialize()

	defer os.RemoveAll(name + kvSuffix)
	defer os.RemoveAll(name)
	err = InitRmq(name, idAllocator)
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()
}

func Test_InitRocksMQ(t *testing.T) {
	rmqPath := "/tmp/milvus/rdb_data_global"
	defer os.RemoveAll("/tmp/milvus")
	err := InitRocksMQ(rmqPath)
	defer Rmq.stopRetention()
	assert.NoError(t, err)
	defer CloseRocksMQ()

	topicName := "topic_register"
	err = Rmq.CreateTopic(topicName)
	assert.NoError(t, err)
	groupName := "group_register"
	_ = Rmq.DestroyConsumerGroup(topicName, groupName)
	err = Rmq.CreateConsumerGroup(topicName, groupName)
	assert.Nil(t, err)

	consumer := &Consumer{
		Topic:     topicName,
		GroupName: groupName,
		MsgMutex:  make(chan struct{}),
	}
	Rmq.RegisterConsumer(consumer)
}

func Test_InitRocksMQError(t *testing.T) {
	once = sync.Once{}
	dir := "/tmp/milvus/"
	dummyPath := dir + "dummy"
	err := os.MkdirAll(dir, os.ModePerm)
	assert.NoError(t, err)
	f, err := os.Create(dummyPath)
	defer f.Close()
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	err = InitRocksMQ(dummyPath)
	assert.Error(t, err)
}
