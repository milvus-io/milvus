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

package rocksmq

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

// Rmq is global rocksmq instance that will be initialized only once
var Rmq *rocksmq

// once is used to init global rocksmq
var once sync.Once

// Params provide params that rocksmq needs
var params paramtable.BaseTable

// InitRmq is deprecate implementation of global rocksmq. will be removed later
func InitRmq(rocksdbName string, idAllocator allocator.GIDAllocator) error {
	var err error
	Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
	return err
}

// InitRocksMQ init global rocksmq single instance
func InitRocksMQ() error {
	params.Init()
	rocksdbName, _ := params.Load("_RocksmqPath")
	log.Debug("initializing global rmq", zap.String("path", rocksdbName))
	rawRmqPageSize, err := params.Load("rocksmq.rocksmqPageSize")
	if err == nil && rawRmqPageSize != "" {
		rmqPageSize, err := strconv.ParseUint(rawRmqPageSize, 10, 64)
		if err != nil {
			log.Warn("rocksmq.rocksmqPageSize is invalid", zap.String("page size", rawRmqPageSize))
			return err
		}
		atomic.StoreUint64(&RocksmqPageSize, rmqPageSize)
	}

	rawRmqRetentionTimeInMinutes, err := params.Load("rocksmq.retentionTimeInMinutes")
	fmt.Println("retention", rawRmqRetentionTimeInMinutes)
	if err == nil && rawRmqRetentionTimeInMinutes != "" {
		rmqRetentionTimeInMinutes, err := strconv.ParseInt(rawRmqRetentionTimeInMinutes, 10, 64)
		fmt.Println("retention", rmqRetentionTimeInMinutes)
		if err != nil {
			log.Warn("rocksmq.retentionTimeInMinutes is invalid", zap.String("retention time", rawRmqRetentionTimeInMinutes))
			return err
		}
		atomic.StoreInt64(&RocksmqRetentionTimeInSecs, rmqRetentionTimeInMinutes*60)
	}
	rawRmqRetentionSizeInMB, err := params.Load("rocksmq.retentionSizeInMB")
	if err == nil && rawRmqRetentionSizeInMB != "" {
		rmqRetentionSizeInMB, err := strconv.ParseInt(rawRmqRetentionSizeInMB, 10, 64)
		if err != nil {
			log.Warn("rocksmq.retentionSizeInMB is invalid", zap.String("retention size", rawRmqRetentionSizeInMB))
			return err
		}
		atomic.StoreInt64(&RocksmqRetentionSizeInMB, rmqRetentionSizeInMB)
	}
	cacheCapacity, err := params.Load("rocksmq.cacheCapacity")
	if err == nil && cacheCapacity != "" {
		cacheCapacityInt, err := strconv.ParseUint(cacheCapacity, 10, 64)
		if err != nil {
			log.Warn("rocksmq.cacheCapacity is invalid", zap.String("cache capacity", cacheCapacity))
			return err
		}
		RocksDBLRUCacheCapacity = cacheCapacityInt
	}
	blockSize, err := params.Load("rocksmq.blockSize")
	if err == nil && blockSize != "" {
		blockSizeInt, err := strconv.ParseInt(blockSize, 10, 64)
		if err != nil {
			log.Warn("rocksmq.blockSize is invalid", zap.String("block size", blockSize))
			return err
		}
		RocksDBBlockSize = int(blockSizeInt)
	}

	log.Debug("RMQ init successfully", zap.Any("RocksmqRetentionTimeInMinutes", rawRmqRetentionTimeInMinutes),
		zap.Any("RocksmqRetentionSizeInMB", RocksmqRetentionSizeInMB),
		zap.Any("RocksmqPageSize", RocksmqPageSize),
		zap.Any("RocksmqCacheSize", RocksDBLRUCacheCapacity),
		zap.Any("RocksmqBlockSize", RocksDBBlockSize),
	)

	var fi os.FileInfo
	fi, err = os.Stat(rocksdbName)
	if os.IsNotExist(err) {
		err = os.MkdirAll(rocksdbName, os.ModePerm)
		if err != nil {
			return err
		}
	} else {
		if !fi.IsDir() {
			errMsg := "can't create a directory because there exists a file with the same name"
			err := errors.New(errMsg)
			return err
		}
	}

	Rmq, err = NewRocksMQ(rocksdbName, nil)
	return err
}

// CloseRocksMQ is used to close global rocksmq
func CloseRocksMQ() {
	log.Debug("Close Rocksmq!")
	if Rmq != nil && Rmq.store != nil {
		Rmq.Close()
	}
}
