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
	"os"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"

	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
)

var Rmq *rocksmq
var once sync.Once
var params paramtable.BaseTable

func InitRmq(rocksdbName string, idAllocator allocator.GIDAllocator) error {
	var err error
	Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
	return err
}

func InitRocksMQ() error {
	var err error
	once.Do(func() {
		params.Init()
		rocksdbName, _ := params.Load("_RocksmqPath")
		log.Debug("RocksmqPath=" + rocksdbName)
		_, err = os.Stat(rocksdbName)
		if os.IsNotExist(err) {
			err = os.MkdirAll(rocksdbName, os.ModePerm)
			if err != nil {
				errMsg := "Create dir " + rocksdbName + " failed"
				panic(errMsg)
			}
		}

		kvname := rocksdbName + "_kv"
		rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvname)
		if err != nil {
			panic(err)
		}
		idAllocator := allocator.NewGlobalIDAllocator("rmq_id", rocksdbKV)
		_ = idAllocator.Initialize()

		atomic.StoreInt64(&RocksmqRetentionTimeInMinutes, params.ParseInt64("rocksmq.retentionTimeInMinutes"))
		atomic.StoreInt64(&RocksmqRetentionSizeInMB, params.ParseInt64("rocksmq.retentionSizeInMB"))
		log.Debug("Rocksmq retention: ", zap.Any("RocksmqRetentionTimeInMinutes", RocksmqRetentionTimeInMinutes), zap.Any("RocksmqRetentionSizeInMB", RocksmqRetentionSizeInMB))
		Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
		if err != nil {
			panic(err)
		}
	})
	return err
}

func CloseRocksMQ() {
	log.Debug("Close Rocksmq!")
	if Rmq != nil {
		Rmq.stopRetention()
		if Rmq.store != nil {
			Rmq.consumers.Range(func(k, v interface{}) bool {
				var topic string
				for _, consumer := range v.([]*Consumer) {
					err := Rmq.DestroyConsumerGroup(consumer.Topic, consumer.GroupName)
					if err != nil {
						log.Warn("Rocksmq DestroyConsumerGroup failed!", zap.Any("topic", consumer.Topic), zap.Any("groupName", consumer.GroupName))
					}
					topic = consumer.Topic
				}
				if topic != "" {
					err := Rmq.DestroyTopic(topic)
					if err != nil {
						log.Warn("Rocksmq DestroyTopic failed!", zap.Any("topic", topic))
					}
				}
				return true
			})
			// FIXME(yukun): When close Rmq.store, there may be some goroutines in rocksmq.Consume() using the
			// store instance, so this may cause crash. Needs to send a mutex to rocksmq to stop using store
			// when Rmq needs to be closed.
			// Rmq.store.Close()
		}
	}
}
