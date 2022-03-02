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

package client

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"
)

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}

func newConsumerName() string {
	return fmt.Sprintf("my-consumer-%v", time.Now().Nanosecond())
}

func newMockRocksMQ() server.RocksMQ {
	var rocksmq server.RocksMQ
	return rocksmq
}

func newMockClient() *client {
	client, _ := newClient(Options{
		Server: newMockRocksMQ(),
	})
	return client
}

func newRocksMQ(t *testing.T, rmqPath string) server.RocksMQ {
	rocksdbPath := rmqPath
	rmq, err := server.NewRocksMQ(rocksdbPath, nil)
	assert.NoError(t, err)
	return rmq
}

func removePath(rmqPath string) {
	// remove path rocksmq created
	rocksdbPath := rmqPath
	err := os.RemoveAll(rocksdbPath)
	if err != nil {
		log.Error("Failed to call os.removeAll.", zap.Any("path", rocksdbPath))
	}
	metaPath := rmqPath + "_meta_kv"
	err = os.RemoveAll(metaPath)
	if err != nil {
		log.Error("Failed to call os.removeAll.", zap.Any("path", metaPath))
	}
}
