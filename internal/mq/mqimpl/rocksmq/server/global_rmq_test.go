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

	"github.com/stretchr/testify/assert"
)

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
	assert.NoError(t, err)

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
