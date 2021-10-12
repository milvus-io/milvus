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

package rocksmq

import (
	"fmt"
	"os"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/internal/log"
	rocksmq "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
	server "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
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
	client, _ := newClient(ClientOptions{
		Server: newMockRocksMQ(),
	})
	return client
}

func initIDAllocator(kvPath string) *allocator.GlobalIDAllocator {
	rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvPath)
	if err != nil {
		panic(err)
	}
	idAllocator := allocator.NewGlobalIDAllocator("rmq_id", rocksdbKV)
	_ = idAllocator.Initialize()
	return idAllocator
}

func newRocksMQ(rmqPath string) server.RocksMQ {
	kvPath := rmqPath + "_kv"
	idAllocator := initIDAllocator(kvPath)

	rocksdbPath := rmqPath + "_db"

	rmq, _ := rocksmq.NewRocksMQ(rocksdbPath, idAllocator)
	return rmq
}

func removePath(rmqPath string) {
	kvPath := rmqPath + "_kv"
	err := os.RemoveAll(kvPath)
	if err != nil {
		log.Error("os removeAll failed.", zap.Any("path", kvPath))
	}
	rocksdbPath := rmqPath + "_db"
	err = os.RemoveAll(rocksdbPath)
	if err != nil {
		log.Error("os removeAll failed.", zap.Any("path", kvPath))
	}
	metaPath := rmqPath + "_meta_kv"
	err = os.RemoveAll(metaPath)
	if err != nil {
		log.Error("os removeAll failed.", zap.Any("path", kvPath))
	}
}
