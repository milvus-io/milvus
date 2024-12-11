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
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/kv"
	rocksdb "github.com/milvus-io/milvus/pkg/kv/rocksdb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is the type of message ID
type UniqueID = typeutil.UniqueID

// RmqState Rocksmq state
type RmqState = int64

// RocksmqPageSize is the size of a message page, default 64MB

// RocksDB cache size limitation(TODO config it)
var RocksDBLRUCacheMinCapacity = uint64(1 << 29)

var RocksDBLRUCacheMaxCapacity = uint64(4 << 30)

// Const variable that will be used in rocksmqs
const (
	DefaultMessageID UniqueID = -1

	kvSuffix = "_meta_kv"

	//  topic_begin_id/topicName
	// topic begin id record a topic is valid, create when topic is created, cleaned up on destroy topic
	TopicIDTitle = "topic_id/"

	// message_size/topicName record the current page message size, once current message size > RocksMq size, reset this value and open a new page
	// TODO should be cached
	MessageSizeTitle = "message_size/"

	// page_message_size/topicName/pageId record the endId of each page, it will be purged either in retention or the destroy of topic
	PageMsgSizeTitle = "page_message_size/"

	// page_ts/topicName/pageId, record the page last ts, used for TTL functionality
	PageTsTitle = "page_ts/"

	// acked_ts/topicName/pageId, record the latest ack ts of each page, will be purged on retention or destroy of the topic
	AckedTsTitle = "acked_ts/"

	RmqNotServingErrMsg = "Rocksmq is not serving"
)

const (
	// RmqStateStopped state stands for just created or stopped `Rocksmq` instance
	RmqStateStopped RmqState = 0
	// RmqStateHealthy state stands for healthy `Rocksmq` instance
	RmqStateHealthy RmqState = 1
)

/**
 * Construct current id
 */
func constructCurrentID(topicName, groupName string) string {
	return groupName + "/" + topicName
}

/**
 * Combine metaname together with topic
 */
func constructKey(metaName, topic string) string {
	// Check metaName/topic
	return metaName + topic
}

func parsePageID(key string) (int64, error) {
	stringSlice := strings.Split(key, "/")
	if len(stringSlice) != 3 {
		return 0, fmt.Errorf("Invalid page id %s ", key)
	}
	return strconv.ParseInt(stringSlice[2], 10, 64)
}

func checkRetention() bool {
	params := paramtable.Get()
	return params.RocksmqCfg.RetentionSizeInMB.GetAsInt64() != -1 || params.RocksmqCfg.RetentionTimeInMinutes.GetAsInt64() != -1
}

var topicMu = sync.Map{}

type rocksmq struct {
	store       *gorocksdb.DB
	cfh         []*gorocksdb.ColumnFamilyHandle
	kv          kv.BaseKV
	storeMu     *sync.Mutex
	consumers   sync.Map
	consumersID sync.Map

	retentionInfo         *retentionInfo
	readers               sync.Map
	state                 RmqState
	topicName2LatestMsgID sync.Map
	ctx                   context.Context
}

func parseCompressionType(params *paramtable.ComponentParam) ([]gorocksdb.CompressionType, error) {
	var tError error
	validType := []int{0, 7}

	return lo.Map(params.RocksmqCfg.CompressionTypes.GetAsStrings(), func(sType string, _ int) gorocksdb.CompressionType {
		iType, err := strconv.Atoi(sType)
		if err != nil {
			tError = fmt.Errorf("invalid rocksmq compression type: %s", err.Error())
			return 0
		}

		if !lo.Contains(validType, iType) {
			tError = fmt.Errorf("invalid rocksmq compression type, should in %v", validType)
			return 0
		}
		return gorocksdb.CompressionType(iType)
	}), tError
}

// NewRocksMQ step:
// 1. New rocksmq instance based on rocksdb with name and rocksdbkv with kvname
// 2. Init retention info, load retention info to memory
// 3. Start retention goroutine
func NewRocksMQ(name string) (*rocksmq, error) {
	params := paramtable.Get()
	// TODO we should use same rocksdb instance with different cfs
	maxProcs := hardware.GetCPUNum()
	parallelism := 1
	if maxProcs > 32 {
		parallelism = 4
	} else if maxProcs > 8 {
		parallelism = 2
	}
	memoryCount := hardware.GetMemoryCount()
	// default rocks db cache is set with memory
	rocksDBLRUCacheCapacity := RocksDBLRUCacheMinCapacity
	if memoryCount > 0 {
		ratio := params.RocksmqCfg.LRUCacheRatio.GetAsFloat()
		calculatedCapacity := uint64(float64(memoryCount) * ratio)
		if calculatedCapacity < RocksDBLRUCacheMinCapacity {
			rocksDBLRUCacheCapacity = RocksDBLRUCacheMinCapacity
		} else if calculatedCapacity > RocksDBLRUCacheMaxCapacity {
			rocksDBLRUCacheCapacity = RocksDBLRUCacheMaxCapacity
		} else {
			rocksDBLRUCacheCapacity = calculatedCapacity
		}
	}
	log.Ctx(context.TODO()).Debug("Start rocksmq", zap.Int("max proc", maxProcs),
		zap.Int("parallism", parallelism), zap.Uint64("lru cache", rocksDBLRUCacheCapacity))
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockSize(64 << 10)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(rocksDBLRUCacheCapacity))

	compressionTypes, err := parseCompressionType(params)
	if err != nil {
		return nil, err
	}

	optsKV := gorocksdb.NewDefaultOptions()
	// L0:No Compression
	// L1,L2: ZSTD
	optsKV.SetNumLevels(len(compressionTypes))
	optsKV.SetCompressionPerLevel(compressionTypes)
	optsKV.SetBlockBasedTableFactory(bbto)
	optsKV.SetTargetFileSizeMultiplier(2)
	optsKV.SetCreateIfMissing(true)
	// by default there are only 1 thread for flush compaction, which may block each other.
	// increase to a reasonable thread numbers
	optsKV.IncreaseParallelism(parallelism)
	// enable back ground flush
	optsKV.SetMaxBackgroundFlushes(1)

	// finish rocks KV
	kvName := name + kvSuffix
	kv, err := rocksdb.NewRocksdbKVWithOpts(kvName, optsKV)
	if err != nil {
		return nil, err
	}

	// finish rocks mq store initialization, rocks mq store has to set the prefix extractor
	optsStore := gorocksdb.NewDefaultOptions()
	// share block cache with kv
	optsStore.SetNumLevels(len(compressionTypes))
	optsStore.SetCompressionPerLevel(compressionTypes)
	optsStore.SetBlockBasedTableFactory(bbto)
	optsStore.SetTargetFileSizeMultiplier(2)
	optsStore.SetCreateIfMissing(true)
	// by default there are only 1 thread for flush compaction, which may block each other.
	// increase to a reasonable thread numbers
	optsStore.IncreaseParallelism(parallelism)
	// enable back ground flush
	optsStore.SetMaxBackgroundFlushes(1)
	// properties is not used anymore, keep it for upgrading successfully
	optsStore.SetCreateIfMissingColumnFamilies(true)

	// db, err := gorocksdb.OpenDb(opts, name)
	// properties is not used anymore, keep it for upgrading successfully
	giveColumnFamilies := []string{"default", "properties"}
	db, cfHandles, err := gorocksdb.OpenDbColumnFamilies(optsStore, name, giveColumnFamilies, []*gorocksdb.Options{optsStore, optsStore})
	if err != nil {
		return nil, err
	}

	ctx := log.WithFields(context.Background(), zap.String("module", "rocksmq"))
	rmq := &rocksmq{
		store:                 db,
		cfh:                   cfHandles,
		kv:                    kv,
		storeMu:               &sync.Mutex{},
		consumers:             sync.Map{},
		readers:               sync.Map{},
		topicName2LatestMsgID: sync.Map{},
		ctx:                   ctx,
	}

	ri, err := initRetentionInfo(kv, db)
	if err != nil {
		return nil, err
	}
	rmq.retentionInfo = ri

	if checkRetention() {
		rmq.retentionInfo.startRetentionInfo()
	}
	atomic.StoreInt64(&rmq.state, RmqStateHealthy)
	// TODO add this to monitor metrics
	go func() {
		for {
			time.Sleep(10 * time.Minute)

			log.Ctx(ctx).Info("Rocksmq stats",
				zap.String("cache", kv.DB.GetProperty("rocksdb.block-cache-usage")),
				zap.String("rockskv memtable ", kv.DB.GetProperty("rocksdb.size-all-mem-tables")),
				zap.String("rockskv table readers", kv.DB.GetProperty("rocksdb.estimate-table-readers-mem")),
				zap.String("rockskv pinned", kv.DB.GetProperty("rocksdb.block-cache-pinned-usage")),
				zap.String("store memtable ", db.GetProperty("rocksdb.size-all-mem-tables")),
				zap.String("store table readers", db.GetProperty("rocksdb.estimate-table-readers-mem")),
				zap.String("store pinned", db.GetProperty("rocksdb.block-cache-pinned-usage")),
				zap.String("store l0 file num", db.GetProperty("rocksdb.num-files-at-level0")),
				zap.String("store l1 file num", db.GetProperty("rocksdb.num-files-at-level1")),
				zap.String("store l2 file num", db.GetProperty("rocksdb.num-files-at-level2")),
				zap.String("store l3 file num", db.GetProperty("rocksdb.num-files-at-level3")),
				zap.String("store l4 file num", db.GetProperty("rocksdb.num-files-at-level4")),
			)
			rmq.Info()
		}
	}()

	return rmq, nil
}

func (rmq *rocksmq) isClosed() bool {
	return atomic.LoadInt64(&rmq.state) != RmqStateHealthy
}

// The format of old key is: topicName/Message. In order to keep the lexicographical order of keys in kv engine,
// new message id still need to use same format by compose method of tsoutil package, it should greater than the
// previous message id as well if the topic already exists.
// return a range value [start, end) if msgIDs are allocated successfully.
func (rmq *rocksmq) allocMsgID(topicName string, delta int) (UniqueID, UniqueID, error) {
	v, ok := rmq.topicName2LatestMsgID.Load(topicName)
	var msgID int64
	if !ok {
		// try to get the latest message id from the topic
		var err error
		msgID, err = rmq.getLatestMsg(topicName)
		if err != nil {
			return 0, 0, err
		}

		if msgID == DefaultMessageID {
			// initialize a new message id if not found the latest msg in the topic
			msgID = UniqueID(tsoutil.ComposeTSByTime(time.Now(), 0))
			log.Ctx(rmq.ctx).Warn("init new message id", zap.String("topicName", topicName), zap.Error(err))
		}
		log.Ctx(rmq.ctx).Info("init the latest message id done", zap.String("topicName", topicName), zap.Int64("msgID", msgID))
	} else {
		msgID = v.(int64)
	}

	newMsgID := msgID + int64(delta)
	rmq.topicName2LatestMsgID.Store(topicName, newMsgID)
	return msgID + 1, newMsgID + 1, nil
}

// Close step:
// 1. Stop retention
// 2. Destroy all consumer groups and topics
// 3. Close rocksdb instance
func (rmq *rocksmq) Close() {
	atomic.StoreInt64(&rmq.state, RmqStateStopped)
	rmq.stopRetention()
	rmq.consumers.Range(func(k, v interface{}) bool {
		// TODO what happened if the server crashed? who handled the destroy consumer group? should we just handled it when rocksmq created?
		// or we should not even make consumer info persistent?
		for _, consumer := range v.([]*Consumer) {
			err := rmq.destroyConsumerGroupInternal(consumer.Topic, consumer.GroupName)
			if err != nil {
				log.Ctx(rmq.ctx).Warn("Failed to destroy consumer group in rocksmq!", zap.String("topic", consumer.Topic), zap.String("groupName", consumer.GroupName), zap.Error(err))
			}
		}
		return true
	})
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	rmq.kv.Close()
	rmq.store.Close()
	log.Ctx(rmq.ctx).Info("Successfully close rocksmq")
}

// print rmq consumer Info
func (rmq *rocksmq) Info() bool {
	log := log.Ctx(rmq.ctx)
	rtn := true
	rmq.consumers.Range(func(key, vals interface{}) bool {
		topic, _ := key.(string)
		consumerList, _ := vals.([]*Consumer)

		minConsumerPosition := UniqueID(-1)
		minConsumerGroupName := ""
		for _, consumer := range consumerList {
			consumerPosition, ok := rmq.getCurrentID(consumer.Topic, consumer.GroupName)
			if !ok {
				log.Error("some group not regist", zap.String("topic", consumer.Topic), zap.String("groupName", consumer.GroupName))
				continue
			}
			if minConsumerPosition == UniqueID(-1) || consumerPosition < minConsumerPosition {
				minConsumerPosition = consumerPosition
				minConsumerGroupName = consumer.GroupName
			}
		}

		pageTsSizeKey := constructKey(PageTsTitle, topic)
		pages, _, err := rmq.kv.LoadWithPrefix(context.TODO(), pageTsSizeKey)
		if err != nil {
			log.Error("Rocksmq get page num failed", zap.String("topic", topic))
			rtn = false
			return false
		}

		msgSizeKey := MessageSizeTitle + topic
		msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
		if err != nil {
			log.Error("Rocksmq get last page size failed", zap.String("topic", topic))
			rtn = false
			return false
		}

		log.Info("Rocksmq Info",
			zap.String("topic", topic),
			zap.Int("consumer num", len(consumerList)),
			zap.String("min position group names", minConsumerGroupName),
			zap.Int64("min positions", minConsumerPosition),
			zap.Int("page sum", len(pages)),
			zap.String("last page size", msgSizeVal),
		)
		return true
	})
	return rtn
}

func (rmq *rocksmq) stopRetention() {
	if rmq.retentionInfo != nil {
		rmq.retentionInfo.Stop()
	}
}

// CreateTopic writes initialized messages for topic in rocksdb
func (rmq *rocksmq) CreateTopic(topicName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()

	log := log.Ctx(rmq.ctx)
	// Check if topicName contains "/"
	if strings.Contains(topicName, "/") {
		log.Warn("rocksmq failed to create topic for topic name contains \"/\"", zap.String("topic", topicName))
		return retry.Unrecoverable(fmt.Errorf("topic name = %s contains \"/\"", topicName))
	}

	// topicIDKey is the only identifier of a topic
	topicIDKey := TopicIDTitle + topicName
	val, err := rmq.kv.Load(context.TODO(), topicIDKey)
	if err != nil {
		return err
	}
	if val != "" {
		log.Warn("rocksmq topic already exists ", zap.String("topic", topicName))
		return nil
	}

	if _, ok := topicMu.Load(topicName); !ok {
		topicMu.Store(topicName, new(sync.Mutex))
	}

	// msgSizeKey -> msgSize
	// topicIDKey -> topic creating time
	kvs := make(map[string]string)

	// Initialize topic message size to 0
	msgSizeKey := MessageSizeTitle + topicName
	kvs[msgSizeKey] = "0"

	// Initialize topic id to its creating time, we don't really use it for now
	nowTs := strconv.FormatInt(time.Now().Unix(), 10)
	kvs[topicIDKey] = nowTs
	if err = rmq.kv.MultiSave(context.TODO(), kvs); err != nil {
		return retry.Unrecoverable(err)
	}

	rmq.retentionInfo.mutex.Lock()
	defer rmq.retentionInfo.mutex.Unlock()
	rmq.retentionInfo.topicRetetionTime.Insert(topicName, time.Now().Unix())
	log.Debug("Rocksmq create topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// DestroyTopic removes messages for topic in rocksmq
func (rmq *rocksmq) DestroyTopic(topicName string) error {
	start := time.Now()
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	rmq.consumers.Delete(topicName)
	rmq.topicName2LatestMsgID.Delete(topicName)

	// clean the topic data it self
	fixTopicName := topicName + "/"
	err := rmq.kv.RemoveWithPrefix(context.TODO(), fixTopicName)
	if err != nil {
		return err
	}

	// clean page size info
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(context.TODO(), pageMsgSizeKey)
	if err != nil {
		return err
	}

	// clean page ts info
	pageMsgTsKey := constructKey(PageTsTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(context.TODO(), pageMsgTsKey)
	if err != nil {
		return err
	}

	// cleaned acked ts info
	ackedTsKey := constructKey(AckedTsTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(context.TODO(), ackedTsKey)
	if err != nil {
		return err
	}

	// topic info
	topicIDKey := TopicIDTitle + topicName
	// message size of this topic
	msgSizeKey := MessageSizeTitle + topicName
	var removedKeys []string
	removedKeys = append(removedKeys, topicIDKey, msgSizeKey)
	// Batch remove, atomic operation
	err = rmq.kv.MultiRemove(context.TODO(), removedKeys)
	if err != nil {
		return err
	}

	// clean up retention info
	topicMu.Delete(topicName)
	rmq.retentionInfo.topicRetetionTime.GetAndRemove(topicName)

	log.Ctx(rmq.ctx).Debug("Rocksmq destroy topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// ExistConsumerGroup check if a consumer exists and return the existed consumer
func (rmq *rocksmq) ExistConsumerGroup(topicName, groupName string) (bool, *Consumer, error) {
	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if ok {
		if vals, ok := rmq.consumers.Load(topicName); ok {
			for _, v := range vals.([]*Consumer) {
				if v.GroupName == groupName {
					return true, v, nil
				}
			}
		}
	}
	return false, nil, nil
}

// CreateConsumerGroup creates an nonexistent consumer group for topic
func (rmq *rocksmq) CreateConsumerGroup(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if ok {
		return fmt.Errorf("RMQ CreateConsumerGroup key already exists, key = %s", key)
	}
	rmq.consumersID.Store(key, DefaultMessageID)
	log.Ctx(rmq.ctx).Debug("Rocksmq create consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// RegisterConsumer registers a consumer in rocksmq consumers
func (rmq *rocksmq) RegisterConsumer(consumer *Consumer) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	if vals, ok := rmq.consumers.Load(consumer.Topic); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == consumer.GroupName {
				return nil
			}
		}
		consumers := vals.([]*Consumer)
		consumers = append(consumers, consumer)
		rmq.consumers.Store(consumer.Topic, consumers)
	} else {
		consumers := make([]*Consumer, 1)
		consumers[0] = consumer
		rmq.consumers.Store(consumer.Topic, consumers)
	}
	log.Ctx(rmq.ctx).Debug("Rocksmq register consumer successfully ", zap.String("topic", consumer.Topic), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (rmq *rocksmq) GetLatestMsg(topicName string) (int64, error) {
	if rmq.isClosed() {
		return DefaultMessageID, errors.New(RmqNotServingErrMsg)
	}
	msgID, err := rmq.getLatestMsg(topicName)
	if err != nil {
		return DefaultMessageID, err
	}

	return msgID, nil
}

// DestroyConsumerGroup removes a consumer group from rocksdb_kv
func (rmq *rocksmq) DestroyConsumerGroup(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	return rmq.destroyConsumerGroupInternal(topicName, groupName)
}

// DestroyConsumerGroup removes a consumer group from rocksdb_kv
func (rmq *rocksmq) destroyConsumerGroupInternal(topicName, groupName string) error {
	start := time.Now()
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()
	key := constructCurrentID(topicName, groupName)
	rmq.consumersID.Delete(key)
	rmq.topicName2LatestMsgID.Delete(topicName)
	if vals, ok := rmq.consumers.Load(topicName); ok {
		consumers := vals.([]*Consumer)
		for index, v := range consumers {
			if v.GroupName == groupName {
				close(v.MsgMutex)
				consumers = append(consumers[:index], consumers[index+1:]...)
				rmq.consumers.Store(topicName, consumers)
				break
			}
		}
	}
	log.Ctx(rmq.ctx).Debug("Rocksmq destroy consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// Produce produces messages for topic and updates page infos for retention
func (rmq *rocksmq) Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error) {
	if messages == nil {
		return []UniqueID{}, fmt.Errorf("messages are empty")
	}
	if rmq.isClosed() {
		return nil, errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return []UniqueID{}, fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return []UniqueID{}, fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	getLockTime := time.Since(start).Milliseconds()

	msgLen := len(messages)
	idStart, idEnd, err := rmq.allocMsgID(topicName, msgLen)
	if err != nil {
		return []UniqueID{}, err
	}
	allocTime := time.Since(start).Milliseconds()
	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}
	// Insert data to store system
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key := path.Join(topicName, strconv.FormatInt(msgID, 10))
		batch.PutCF(rmq.cfh[0], []byte(key), messages[i].Payload)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
	err = rmq.store.Write(opts, batch)
	if err != nil {
		return []UniqueID{}, err
	}
	writeTime := time.Since(start).Milliseconds()
	if vals, ok := rmq.consumers.Load(topicName); ok {
		for _, v := range vals.([]*Consumer) {
			select {
			case v.MsgMutex <- struct{}{}:
				continue
			default:
				continue
			}
		}
	}

	// Update message page info
	err = rmq.updatePageInfo(topicName, msgIDs, msgSizes)
	if err != nil {
		return []UniqueID{}, err
	}

	// TODO add this to monitor metrics
	getProduceTime := time.Since(start).Milliseconds()
	if getProduceTime > 200 {
		log.Ctx(rmq.ctx).Warn("rocksmq produce too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("alloc elapse", allocTime-getLockTime),
			zap.Int64("write elapse", writeTime-allocTime),
			zap.Int64("updatePage elapse", getProduceTime-writeTime),
			zap.Int64("produce total elapse", getProduceTime),
		)
	}

	return msgIDs, nil
}

func (rmq *rocksmq) updatePageInfo(topicName string, msgIDs []UniqueID, msgSizes map[UniqueID]int64) error {
	params := paramtable.Get()
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(context.TODO(), msgSizeKey)
	if err != nil {
		return err
	}
	curMsgSize, err := strconv.ParseInt(msgSizeVal, 10, 64)
	if err != nil {
		return err
	}
	fixedPageSizeKey := constructKey(PageMsgSizeTitle, topicName)
	fixedPageTsKey := constructKey(PageTsTitle, topicName)
	nowTs := strconv.FormatInt(time.Now().Unix(), 10)
	mutateBuffer := make(map[string]string)
	for _, id := range msgIDs {
		msgSize := msgSizes[id]
		if curMsgSize+msgSize > params.RocksmqCfg.PageSize.GetAsInt64() {
			// Current page is full
			newPageSize := curMsgSize + msgSize
			pageEndID := id
			// Update page message size for current page. key is page end ID
			pageMsgSizeKey := fixedPageSizeKey + "/" + strconv.FormatInt(pageEndID, 10)
			mutateBuffer[pageMsgSizeKey] = strconv.FormatInt(newPageSize, 10)
			pageTsKey := fixedPageTsKey + "/" + strconv.FormatInt(pageEndID, 10)
			mutateBuffer[pageTsKey] = nowTs
			curMsgSize = 0
		} else {
			curMsgSize += msgSize
		}
	}
	mutateBuffer[msgSizeKey] = strconv.FormatInt(curMsgSize, 10)
	err = rmq.kv.MultiSave(context.TODO(), mutateBuffer)
	return err
}

func (rmq *rocksmq) getCurrentID(topicName, groupName string) (int64, bool) {
	currentID, ok := rmq.consumersID.Load(constructCurrentID(topicName, groupName))
	if !ok {
		return 0, false
	}
	return currentID.(int64), true
}

func (rmq *rocksmq) getLastID(topicName string) (int64, bool) {
	currentID, ok := rmq.consumersID.Load(topicName)
	if !ok {
		return 0, false
	}
	return currentID.(int64), true
}

// Consume steps:
// 1. Consume n messages from rocksdb
// 2. Update current_id to the last consumed message
// 3. Update ack informations in rocksdb
func (rmq *rocksmq) Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error) {
	if rmq.isClosed() {
		return nil, errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return nil, fmt.Errorf("topic name = %s not exist", topicName)
	}

	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return nil, fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	currentID, ok := rmq.getCurrentID(topicName, groupName)
	if !ok {
		return nil, fmt.Errorf("currentID of topicName=%s, groupName=%s not exist", topicName, groupName)
	}

	// return if don't have new message
	lastID, ok := rmq.getLastID(topicName)
	if ok && currentID > lastID {
		return []ConsumerMessage{}, nil
	}

	getLockTime := time.Since(start).Milliseconds()
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	prefix := topicName + "/"
	iter := rocksdb.NewRocksIteratorCFWithUpperBound(rmq.store, rmq.cfh[0], typeutil.AddOne(prefix), readOpts)
	defer iter.Close()

	var dataKey string
	if currentID == DefaultMessageID {
		dataKey = prefix
	} else {
		dataKey = path.Join(topicName, strconv.FormatInt(currentID, 10))
	}

	iter.Seek([]byte(dataKey))

	consumerMessage := make([]ConsumerMessage, 0, n)
	offset := 0

	for ; iter.Valid() && offset < n; iter.Next() {
		key := iter.Key()
		val := iter.Value()
		key.Free()

		strKey := string(key.Data())
		msgID, err := strconv.ParseInt(strKey[len(topicName)+1:], 10, 64)
		if err != nil {
			val.Free()
			return nil, err
		}
		offset++

		msg := ConsumerMessage{
			MsgID: msgID,
		}
		origData := val.Data()
		dataLen := len(origData)
		if dataLen == 0 {
			msg.Payload = nil
		} else {
			msg.Payload = make([]byte, dataLen)
			copy(msg.Payload, origData)
		}
		consumerMessage = append(consumerMessage, msg)
		val.Free()
	}
	// if iterate fail
	if err := iter.Err(); err != nil {
		return nil, err
	}
	iterTime := time.Since(start).Milliseconds()

	// When already consume to last mes, an empty slice will be returned
	if len(consumerMessage) == 0 {
		// log.Debug("RocksMQ: consumerMessage is empty")
		return consumerMessage, nil
	}

	newID := consumerMessage[len(consumerMessage)-1].MsgID
	moveConsumePosTime := time.Since(start).Milliseconds()

	err := rmq.moveConsumePos(topicName, groupName, newID+1)
	if err != nil {
		return nil, err
	}

	// TODO add this to monitor metrics
	getConsumeTime := time.Since(start).Milliseconds()
	if getConsumeTime > 200 {
		log.Ctx(rmq.ctx).Warn("rocksmq consume too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("iterator elapse", iterTime-getLockTime),
			zap.Int64("moveConsumePosTime elapse", moveConsumePosTime-iterTime),
			zap.Int64("total consume elapse", getConsumeTime))
	}
	return consumerMessage, nil
}

// seek is used for internal call without the topicMu
func (rmq *rocksmq) seek(topicName string, groupName string, msgID UniqueID) error {
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	storeKey := path.Join(topicName, strconv.FormatInt(msgID, 10))
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	val, err := rmq.store.Get(opts, []byte(storeKey))
	if err != nil {
		return err
	}
	defer val.Free()
	if !val.Exists() {
		log.Ctx(rmq.ctx).Warn("RocksMQ: trying to seek to no exist position, reset current id",
			zap.String("topic", topicName), zap.String("group", groupName), zap.Int64("msgId", msgID))
		err := rmq.moveConsumePos(topicName, groupName, DefaultMessageID)
		// skip seek if key is not found, this is the behavior as pulsar
		return err
	}
	/* Step II: update current_id */
	err = rmq.moveConsumePos(topicName, groupName, msgID)
	return err
}

func (rmq *rocksmq) moveConsumePos(topicName string, groupName string, msgID UniqueID) error {
	oldPos, ok := rmq.getCurrentID(topicName, groupName)
	if !ok {
		return errors.New("move unknown consumer")
	}

	log := log.Ctx(rmq.ctx)
	if msgID < oldPos {
		log.Warn("RocksMQ: trying to move Consume position backward",
			zap.String("topic", topicName), zap.String("group", groupName), zap.Int64("oldPos", oldPos), zap.Int64("newPos", msgID))
		panic("move consume position backward")
	}

	// update ack if position move forward
	err := rmq.updateAckedInfo(topicName, groupName, oldPos, msgID-1)
	if err != nil {
		log.Warn("failed to update acked info ", zap.String("topic", topicName),
			zap.String("groupName", groupName), zap.Error(err))
		return err
	}

	rmq.consumersID.Store(constructCurrentID(topicName, groupName), msgID)
	return nil
}

// Seek updates the current id to the given msgID
func (rmq *rocksmq) Seek(topicName string, groupName string, msgID UniqueID) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	/* Step I: Check if key exists */
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return merr.WrapErrMqTopicNotFound(topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	err := rmq.seek(topicName, groupName, msgID)
	if err != nil {
		return err
	}
	log.Ctx(rmq.ctx).Debug("successfully seek", zap.String("topic", topicName), zap.String("group", groupName), zap.Uint64("msgId", uint64(msgID)))
	return nil
}

// Only for test
func (rmq *rocksmq) ForceSeek(topicName string, groupName string, msgID UniqueID) error {
	log := log.Ctx(rmq.ctx)
	log.Warn("Use method ForceSeek that only for test")
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	/* Step I: Check if key exists */
	ll, ok := topicMu.Load(topicName)
	if !ok {
		return merr.WrapErrMqTopicNotFound(topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()

	key := constructCurrentID(topicName, groupName)
	_, ok = rmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	rmq.consumersID.Store(key, msgID)

	log.Debug("successfully force seek", zap.String("topic", topicName),
		zap.String("group", groupName), zap.Uint64("msgID", uint64(msgID)))
	return nil
}

// SeekToLatest updates current id to the msg id of latest message + 1
func (rmq *rocksmq) SeekToLatest(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()

	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	msgID, err := rmq.getLatestMsg(topicName)
	if err != nil {
		return err
	}

	// current msgID should not be included
	err = rmq.moveConsumePos(topicName, groupName, msgID+1)
	if err != nil {
		return err
	}

	log.Ctx(rmq.ctx).Debug("successfully seek to latest", zap.String("topic", topicName),
		zap.String("group", groupName), zap.Uint64("latest", uint64(msgID+1)))
	return nil
}

func (rmq *rocksmq) getLatestMsg(topicName string) (int64, error) {
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	iter := rocksdb.NewRocksIteratorCF(rmq.store, rmq.cfh[0], readOpts)
	defer iter.Close()

	prefix := topicName + "/"
	// seek to the last message of the topic
	iter.SeekForPrev([]byte(typeutil.AddOne(prefix)))

	// if iterate fail
	if err := iter.Err(); err != nil {
		return DefaultMessageID, err
	}
	// should find the last key we written into, start with fixTopicName/
	// if not find, start from 0
	if !iter.Valid() {
		return DefaultMessageID, nil
	}

	iKey := iter.Key()
	seekMsgID := string(iKey.Data())
	if iKey != nil {
		iKey.Free()
	}

	// if find message is not belong to current channel, start from 0
	if !strings.Contains(seekMsgID, prefix) {
		return DefaultMessageID, nil
	}

	msgID, err := strconv.ParseInt(seekMsgID[len(topicName)+1:], 10, 64)
	if err != nil {
		return DefaultMessageID, err
	}

	return msgID, nil
}

// Notify sends a mutex in MsgMutex channel to tell consumers to consume
func (rmq *rocksmq) Notify(topicName, groupName string) {
	if vals, ok := rmq.consumers.Load(topicName); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == groupName {
				select {
				case v.MsgMutex <- struct{}{}:
					continue
				default:
					continue
				}
			}
		}
	}
}

// updateAckedInfo update acked informations for retention after consume
func (rmq *rocksmq) updateAckedInfo(topicName, groupName string, firstID UniqueID, lastID UniqueID) error {
	log := log.Ctx(rmq.ctx)
	// 1. Try to get the page id between first ID and last ID of ids
	pageMsgPrefix := constructKey(PageMsgSizeTitle, topicName) + "/"
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	pageMsgFirstKey := pageMsgPrefix + strconv.FormatInt(firstID, 10)

	iter := rocksdb.NewRocksIteratorWithUpperBound(rmq.kv.(*rocksdb.RocksdbKV).DB, typeutil.AddOne(pageMsgPrefix), readOpts)
	defer iter.Close()
	var pageIDs []UniqueID

	for iter.Seek([]byte(pageMsgFirstKey)); iter.Valid(); iter.Next() {
		key := iter.Key()
		pageID, err := parsePageID(string(key.Data()))
		if key != nil {
			key.Free()
		}
		if err != nil {
			return err
		}
		if pageID <= lastID {
			pageIDs = append(pageIDs, pageID)
		} else {
			break
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(pageIDs) == 0 {
		return nil
	}
	fixedAckedTsKey := constructKey(AckedTsTitle, topicName)

	// 2. Update acked ts and acked size for pageIDs
	if vals, ok := rmq.consumers.Load(topicName); ok {
		consumers, ok := vals.([]*Consumer)
		if !ok || len(consumers) == 0 {
			log.Error("update ack with no consumer", zap.String("topic", topicName))
			return nil
		}

		// find min id of all consumer
		var minBeginID UniqueID = lastID
		for _, consumer := range consumers {
			if consumer.GroupName != groupName {
				beginID, ok := rmq.getCurrentID(consumer.Topic, consumer.GroupName)
				if !ok {
					return fmt.Errorf("currentID of topicName=%s, groupName=%s not exist", consumer.Topic, consumer.GroupName)
				}
				if beginID < minBeginID {
					minBeginID = beginID
				}
			}
		}

		nowTs := strconv.FormatInt(time.Now().Unix(), 10)
		ackedTsKvs := make(map[string]string)
		// update ackedTs, if page is all acked, then ackedTs is set
		for _, pID := range pageIDs {
			if pID <= minBeginID {
				// Update acked info for message pID
				pageAckedTsKey := path.Join(fixedAckedTsKey, strconv.FormatInt(pID, 10))
				ackedTsKvs[pageAckedTsKey] = nowTs
			}
		}
		err := rmq.kv.MultiSave(context.TODO(), ackedTsKvs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rmq *rocksmq) CheckTopicValid(topic string) error {
	_, ok := topicMu.Load(topic)
	if !ok {
		return merr.WrapErrMqTopicNotFound(topic, "failed to get topic")
	}

	_, err := rmq.GetLatestMsg(topic)
	if err != nil {
		return err
	}
	return nil
}
