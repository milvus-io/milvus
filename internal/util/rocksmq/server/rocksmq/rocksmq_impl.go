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
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"
)

// UniqueID is the type of message ID
type UniqueID = typeutil.UniqueID

// RmqState Rocksmq state
type RmqState = int64

// RocksmqPageSize is the size of a message page, default 2GB
var RocksmqPageSize int64 = 2 << 30

// Const variable that will be used in rocksmqs
const (
	DefaultMessageID        = "-1"
	FixedChannelNameLen     = 320
	RocksDBLRUCacheCapacity = 0

	kvSuffix = "_meta_kv"

	MessageSizeTitle  = "message_size/"
	PageMsgSizeTitle  = "page_message_size/"
	TopicBeginIDTitle = "topic_begin_id/"
	BeginIDTitle      = "begin_id/"
	AckedTsTitle      = "acked_ts/"
	AckedSizeTitle    = "acked_size/"
	LastRetTsTitle    = "last_retention_ts/"

	CurrentIDSuffix  = "current_id"
	ReaderNamePrefix = "reader-"

	RmqNotServingErrMsg = "rocksmq is not serving"
)

const (
	// RmqStateStopped state stands for just created or stopped `Rocksmq` instance
	RmqStateStopped RmqState = 0
	// RmqStateHealthy state stands for healthy `Rocksmq` instance
	RmqStateHealthy RmqState = 1
)

/**
 * @brief fill with '_' to ensure channel name fixed length
 */
func fixChannelName(name string) (string, error) {
	if len(name) > FixedChannelNameLen {
		return "", errors.New("Channel name exceeds limit")
	}

	nameBytes := make([]byte, FixedChannelNameLen-len(name))

	for i := 0; i < len(nameBytes); i++ {
		nameBytes[i] = byte('*')
	}

	return name + string(nameBytes), nil
}

/**
 * Combine key with fixed channel name and unique id
 */
func combKey(channelName string, id UniqueID) (string, error) {
	fixName, err := fixChannelName(channelName)
	if err != nil {
		return "", err
	}

	return fixName + "/" + strconv.FormatInt(id, 10), nil
}

/**
 * Construct current id
 */
func constructCurrentID(topicName, groupName string) string {
	return groupName + "/" + topicName + "/" + CurrentIDSuffix
}

/**
 * Construct table name and fixed channel name to be a key with length of FixedChannelNameLen,
 * used for meta infos
 */
func constructKey(metaName, topic string) (string, error) {
	// Check metaName/topic
	oldLen := len(metaName + topic)
	if oldLen > FixedChannelNameLen {
		return "", errors.New("topic name exceeds limit")
	}

	nameBytes := make([]byte, FixedChannelNameLen-oldLen)

	for i := 0; i < len(nameBytes); i++ {
		nameBytes[i] = byte('*')
	}
	return metaName + topic + string(nameBytes), nil
}

func checkRetention() bool {
	return RocksmqRetentionTimeInMinutes != -1 && RocksmqRetentionSizeInMB != -1
}

func getNowTs(idAllocator allocator.GIDAllocator) (int64, error) {
	err := idAllocator.UpdateID()
	if err != nil {
		return 0, err
	}
	newID, err := idAllocator.AllocOne()
	if err != nil {
		return 0, err
	}
	nowTs, _ := tsoutil.ParseTS(uint64(newID))
	return nowTs.Unix(), err
}

var topicMu sync.Map = sync.Map{}

type rocksmq struct {
	store       *gorocksdb.DB
	kv          kv.BaseKV
	idAllocator allocator.GIDAllocator
	storeMu     *sync.Mutex
	consumers   sync.Map
	ackedMu     sync.Map

	retentionInfo *retentionInfo
	readers       sync.Map
	state         RmqState
}

// NewRocksMQ step:
// 1. New rocksmq instance based on rocksdb with name and rocksdbkv with kvname
// 2. Init retention info, load retention info to memory
// 3. Start retention goroutine
func NewRocksMQ(name string, idAllocator allocator.GIDAllocator) (*rocksmq, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(RocksDBLRUCacheCapacity))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(FixedChannelNameLen + 1))
	// opts.SetMaxOpenFiles(-1)

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}

	kvName := name + kvSuffix
	kv, err := rocksdbkv.NewRocksdbKV(kvName)
	if err != nil {
		return nil, err
	}

	rmq := &rocksmq{
		store:       db,
		kv:          kv,
		idAllocator: idAllocator,
		storeMu:     &sync.Mutex{},
		consumers:   sync.Map{},
		ackedMu:     sync.Map{},
		readers:     sync.Map{},
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
	return rmq, nil
}

func (rmq *rocksmq) isClosed() bool {
	return atomic.LoadInt64(&rmq.state) != RmqStateHealthy
}

// Close step:
// 1. Stop retention
// 2. Destroy all consumer groups and topics
// 3. Close rocksdb instance
func (rmq *rocksmq) Close() {
	atomic.StoreInt64(&rmq.state, RmqStateStopped)
	rmq.stopRetention()
	rmq.consumers.Range(func(k, v interface{}) bool {
		var topic string
		for _, consumer := range v.([]*Consumer) {
			err := rmq.DestroyConsumerGroup(consumer.Topic, consumer.GroupName)
			if err != nil {
				log.Warn("Failed to destroy consumer group in rocksmq!", zap.Any("topic", consumer.Topic), zap.Any("groupName", consumer.GroupName), zap.Any("error", err))
			}
			topic = consumer.Topic
		}
		if topic != "" {
			err := rmq.DestroyTopic(topic)
			if err != nil {
				log.Warn("Rocksmq DestroyTopic failed!", zap.Any("topic", topic), zap.Any("error", err))
			}
		}
		return true
	})
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	rmq.store.Close()
}

func (rmq *rocksmq) stopRetention() {
	if rmq.retentionInfo != nil {
		rmq.retentionInfo.Stop()
	}
}

func (rmq *rocksmq) checkKeyExist(key string) bool {
	val, _ := rmq.kv.Load(key)
	return val != ""
}

// CreateTopic writes initialized messages for topic in rocksdb
func (rmq *rocksmq) CreateTopic(topicName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	beginKey := topicName + "/begin_id"
	endKey := topicName + "/end_id"

	// Check if topic exist
	if rmq.checkKeyExist(beginKey) || rmq.checkKeyExist(endKey) {
		log.Warn("RocksMQ: " + beginKey + " or " + endKey + " existed.")
		return nil
	}
	// TODO change rmq kv save logic into a batch
	err := rmq.kv.Save(beginKey, "0")
	if err != nil {
		return err
	}

	err = rmq.kv.Save(endKey, "0")
	if err != nil {
		return err
	}
	if _, ok := topicMu.Load(topicName); !ok {
		topicMu.Store(topicName, new(sync.Mutex))
	}
	if _, ok := rmq.ackedMu.Load(topicName); !ok {
		rmq.ackedMu.Store(topicName, new(sync.Mutex))
	}

	// Initialize retention infos
	// Initialize acked size to 0 for topic
	ackedSizeKey := AckedSizeTitle + topicName
	err = rmq.kv.Save(ackedSizeKey, "0")
	if err != nil {
		return err
	}

	// Initialize topic begin id to defaultMessageID
	topicBeginIDKey := TopicBeginIDTitle + topicName
	err = rmq.kv.Save(topicBeginIDKey, DefaultMessageID)
	if err != nil {
		return err
	}

	// Initialize topic message size to 0
	msgSizeKey := MessageSizeTitle + topicName
	err = rmq.kv.Save(msgSizeKey, "0")
	if err != nil {
		return err
	}

	rmq.retentionInfo.mutex.Lock()
	defer rmq.retentionInfo.mutex.Unlock()
	rmq.retentionInfo.topics.Store(topicName, time.Now().Unix())
	log.Debug("Rocksmq create topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// DestroyTopic removes messages for topic in rocksdb
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
	beginKey := topicName + "/begin_id"
	endKey := topicName + "/end_id"
	var removedKeys []string

	rmq.consumers.Delete(topicName)

	ackedSizeKey := AckedSizeTitle + topicName
	topicBeginIDKey := TopicBeginIDTitle + topicName

	// just for clean up old topics, for new topics this is not required
	lastRetTsKey := LastRetTsTitle + topicName
	msgSizeKey := MessageSizeTitle + topicName

	removedKeys = append(removedKeys, beginKey, endKey, ackedSizeKey, topicBeginIDKey, lastRetTsKey, msgSizeKey)
	// Batch remove, atomic operation
	err := rmq.kv.MultiRemove(removedKeys)
	if err != nil {
		return err
	}

	// clean up retention info
	topicMu.Delete(topicName)
	rmq.retentionInfo.topics.Delete(topicName)

	// clean up reader
	if val, ok := rmq.readers.LoadAndDelete(topicName); ok {
		for _, reader := range val.([]*rocksmqReader) {
			reader.Close()
		}
	}
	log.Debug("Rocksmq destroy topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// ExistConsumerGroup check if a consumer exists and return the existed consumer
func (rmq *rocksmq) ExistConsumerGroup(topicName, groupName string) (bool, *Consumer) {
	key := constructCurrentID(topicName, groupName)
	if rmq.checkKeyExist(key) {
		if vals, ok := rmq.consumers.Load(topicName); ok {
			for _, v := range vals.([]*Consumer) {
				if v.GroupName == groupName {
					return true, v
				}
			}
		}
	}
	return false, nil
}

// CreateConsumerGroup creates an nonexistent consumer group for topic
func (rmq *rocksmq) CreateConsumerGroup(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()
	key := constructCurrentID(topicName, groupName)
	if rmq.checkKeyExist(key) {
		log.Debug("RMQ CreateConsumerGroup key already exists", zap.String("key", key))
		return nil
	}
	err := rmq.kv.Save(key, DefaultMessageID)
	if err != nil {
		return err
	}
	log.Debug("Rocksmq create consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// RegisterConsumer registers a consumer in rocksmq consumers
func (rmq *rocksmq) RegisterConsumer(consumer *Consumer) {
	if rmq.isClosed() {
		return
	}
	start := time.Now()
	if vals, ok := rmq.consumers.Load(consumer.Topic); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == consumer.GroupName {
				return
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
	log.Debug("Rocksmq register consumer successfully ", zap.String("topic", consumer.Topic), zap.Int64("elapsed", time.Since(start).Milliseconds()))
}

// DestroyConsumerGroup removes a consumer group from rocksdb_kv
func (rmq *rocksmq) DestroyConsumerGroup(topicName, groupName string) error {
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

	err := rmq.kv.Remove(key)
	if err != nil {
		return err
	}
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
	log.Debug("Rocksmq destroy consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// Produce produces messages for topic and updates page infos for retention
func (rmq *rocksmq) Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error) {
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
	idStart, idEnd, err := rmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		log.Error("RocksMQ: alloc id failed.", zap.Error(err))
		return []UniqueID{}, err
	}

	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}

	/* Step I: Insert data to store system */
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key, err := combKey(topicName, msgID)
		if err != nil {
			return []UniqueID{}, err
		}

		batch.Put([]byte(key), messages[i].Payload)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
	err = rmq.store.Write(opts, batch)
	if err != nil {
		log.Debug("RocksMQ: write batch failed")
		return []UniqueID{}, err
	}

	/* Step II: Update meta data to kv system */
	kvChannelBeginID := topicName + "/begin_id"
	beginIDValue, err := rmq.kv.Load(kvChannelBeginID)
	if err != nil {
		log.Debug("RocksMQ: load " + kvChannelBeginID + " failed")
		return []UniqueID{}, err
	}

	kvValues := make(map[string]string)

	if beginIDValue == "0" {
		kvValues[kvChannelBeginID] = strconv.FormatInt(idStart, 10)
	}

	kvChannelEndID := topicName + "/end_id"
	kvValues[kvChannelEndID] = strconv.FormatInt(idEnd, 10)

	err = rmq.kv.MultiSave(kvValues)
	if err != nil {
		log.Debug("RocksMQ: multisave failed")
		return []UniqueID{}, err
	}

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

	// Notify reader
	if val, ok := rmq.readers.Load(topicName); ok {
		for _, reader := range val.([]*rocksmqReader) {
			select {
			case reader.readerMutex <- struct{}{}:
			default:
			}
		}
	}

	// Update message page info
	// TODO(yukun): Should this be in a go routine
	err = rmq.updatePageInfo(topicName, msgIDs, msgSizes)
	if err != nil {
		return []UniqueID{}, err
	}

	getProduceTime := time.Since(start).Milliseconds()
	if getLockTime > 200 || getProduceTime > 200 {
		log.Warn("rocksmq produce too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime), zap.Int64("produce elapse", getProduceTime))
	}
	return msgIDs, nil
}

func (rmq *rocksmq) updatePageInfo(topicName string, msgIDs []UniqueID, msgSizes map[UniqueID]int64) error {
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
	if err != nil {
		return err
	}
	curMsgSize, err := strconv.ParseInt(msgSizeVal, 10, 64)
	if err != nil {
		return err
	}
	fixedPageSizeKey, err := constructKey(PageMsgSizeTitle, topicName)
	if err != nil {
		return err
	}
	for _, id := range msgIDs {
		msgSize := msgSizes[id]
		if curMsgSize+msgSize > RocksmqPageSize {
			// Current page is full
			newPageSize := curMsgSize + msgSize
			pageEndID := id
			// Update page message size for current page. key is page end ID
			pageMsgSizeKey := fixedPageSizeKey + "/" + strconv.FormatInt(pageEndID, 10)
			err := rmq.kv.Save(pageMsgSizeKey, strconv.FormatInt(newPageSize, 10))
			if err != nil {
				return err
			}
			curMsgSize = 0
		} else {
			curMsgSize += msgSize
		}
	}
	// Update message size to current message size
	err = rmq.kv.Save(msgSizeKey, strconv.FormatInt(curMsgSize, 10))
	return err
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
	getLockTime := time.Since(start).Milliseconds()

	metaKey := constructCurrentID(topicName, groupName)
	currentID, err := rmq.kv.Load(metaKey)
	if err != nil {
		log.Debug("RocksMQ: load " + metaKey + " failed")
		return nil, err
	}
	if currentID == "" {
		return nil, fmt.Errorf("currentID of topicName=%s, groupName=%s not exist", topicName, groupName)
	}

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetPrefixSameAsStart(true)
	iter := rmq.store.NewIterator(readOpts)
	defer iter.Close()

	consumerMessage := make([]ConsumerMessage, 0, n)

	fixChanName, err := fixChannelName(topicName)
	if err != nil {
		log.Debug("RocksMQ: fixChannelName " + topicName + " failed")
		return nil, err
	}

	var dataKey string
	if currentID == DefaultMessageID {
		dataKey = fixChanName + "/"
	} else {
		dataKey = fixChanName + "/" + currentID
	}
	iter.Seek([]byte(dataKey))

	offset := 0
	for ; iter.Valid() && offset < n; iter.Next() {
		key := iter.Key()
		val := iter.Value()
		strKey := string(key.Data())
		key.Free()
		offset++
		msgID, err := strconv.ParseInt(strKey[FixedChannelNameLen+1:], 10, 64)
		if err != nil {
			log.Debug("RocksMQ: parse int " + strKey[FixedChannelNameLen+1:] + " failed")
			val.Free()
			return nil, err
		}
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

	// When already consume to last mes, an empty slice will be returned
	if len(consumerMessage) == 0 {
		// log.Debug("RocksMQ: consumerMessage is empty")
		return consumerMessage, nil
	}

	consumedIDs := make([]UniqueID, 0, len(consumerMessage))
	for _, msg := range consumerMessage {
		consumedIDs = append(consumedIDs, msg.MsgID)
	}
	newID := consumedIDs[len(consumedIDs)-1]
	err = rmq.moveConsumePos(topicName, groupName, newID+1)
	if err != nil {
		return nil, err
	}

	go rmq.updateAckedInfo(topicName, groupName, consumedIDs)
	getConsumeTime := time.Since(start).Milliseconds()
	if getLockTime > 200 || getConsumeTime > 200 {
		log.Warn("rocksmq consume too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime), zap.Int64("consume elapse", getConsumeTime))
	}
	return consumerMessage, nil
}

// seek is used for internal call without the topicMu
func (rmq *rocksmq) seek(topicName string, groupName string, msgID UniqueID) error {
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	if !rmq.checkKeyExist(key) {
		log.Warn("RocksMQ: channel " + key + " not exists")
		return fmt.Errorf("consumerGroup %s, channel %s not exists", groupName, topicName)
	}
	storeKey, err := combKey(topicName, msgID)
	if err != nil {
		log.Warn("RocksMQ: combKey(" + topicName + "," + strconv.FormatInt(msgID, 10) + ") failed")
		return err
	}
	opts := gorocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	val, err := rmq.store.Get(opts, []byte(storeKey))
	defer val.Free()
	if err != nil {
		log.Warn("RocksMQ: get " + storeKey + " failed")
		return err
	}
	if !val.Exists() {
		//skip seek if key is not found, this is the behavior as pulsar
		return nil
	}

	/* Step II: Save current_id in kv */
	return rmq.moveConsumePos(topicName, groupName, msgID)
}

func (rmq *rocksmq) moveConsumePos(topicName string, groupName string, msgID UniqueID) error {
	key := constructCurrentID(topicName, groupName)
	err := rmq.kv.Save(key, strconv.FormatInt(msgID, 10))
	if err != nil {
		log.Warn("RocksMQ: save " + key + " failed")
		return err
	}
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
		return fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	return rmq.seek(topicName, groupName, msgID)
}

// SeekToLatest updates current id to the msg id of latest message + 1
func (rmq *rocksmq) SeekToLatest(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	if !rmq.checkKeyExist(key) {
		log.Debug("RocksMQ: channel " + key + " not exists")
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	iter := rmq.store.NewIterator(readOpts)
	defer iter.Close()

	fixChanName, _ := fixChannelName(topicName)

	// 0 is the ASC value of "/" + 1
	iter.SeekForPrev([]byte(fixChanName + "0"))

	// should find the last key we written into, start with fixChanName/
	// if not find, start from 0
	if !iter.Valid() {
		return nil
	}

	iKey := iter.Key()
	seekMsgID := string(iKey.Data())
	iKey.Free()
	// if find message is not belong to current channel, start from 0
	if !strings.Contains(seekMsgID, fixChanName+"/") {
		return nil
	}

	msgID, err := strconv.ParseInt(seekMsgID[FixedChannelNameLen+1:], 10, 64)
	if err != nil {
		return err
	}
	// current msgID should not be included
	return rmq.moveConsumePos(topicName, groupName, msgID+1)
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
func (rmq *rocksmq) updateAckedInfo(topicName, groupName string, ids []UniqueID) error {
	if len(ids) == 0 {
		return nil
	}
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

	firstID := ids[0]
	lastID := ids[len(ids)-1]

	fixedBeginIDKey, err := constructKey(BeginIDTitle, topicName)
	if err != nil {
		return err
	}

	// 1. Update begin_id for the consumer_group
	beginIDKey := fixedBeginIDKey + "/" + groupName
	err = rmq.kv.Save(beginIDKey, strconv.FormatInt(lastID, 10))
	if err != nil {
		return err
	}

	// 2. Try to get the page id between first ID and last ID of ids
	pageMsgPrefix, err := constructKey(PageMsgSizeTitle, topicName)
	if err != nil {
		return err
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetPrefixSameAsStart(true)
	iter := rmq.kv.(*rocksdbkv.RocksdbKV).DB.NewIterator(readOpts)
	defer iter.Close()

	var pageIDs []UniqueID
	pageMsgKey := pageMsgPrefix + "/" + strconv.FormatInt(firstID, 10)
	for iter.Seek([]byte(pageMsgKey)); iter.Valid(); iter.Next() {
		key := iter.Key()
		pageID, err := strconv.ParseInt(string(key.Data())[FixedChannelNameLen+1:], 10, 64)
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
	if len(pageIDs) == 0 {
		return nil
	}

	fixedAckedTsKey, err := constructKey(AckedTsTitle, topicName)
	if err != nil {
		return err
	}

	// 3. Update acked ts and acked size for pageIDs
	if vals, ok := rmq.consumers.Load(topicName); ok {
		var minBeginID int64 = math.MaxInt64
		consumers, ok := vals.([]*Consumer)
		if !ok || len(consumers) == 0 {
			return nil
		}
		for _, v := range consumers {
			curBeginIDKey := path.Join(fixedBeginIDKey, v.GroupName)
			curBeginIDVal, err := rmq.kv.Load(curBeginIDKey)
			if err != nil {
				return err
			}
			curBeginID, err := strconv.ParseInt(curBeginIDVal, 10, 64)
			if err != nil {
				return err
			}
			if curBeginID < minBeginID {
				minBeginID = curBeginID
			}
		}

		nowTs := strconv.FormatInt(time.Now().Unix(), 10)
		ackedTsKvs := make(map[string]string)
		totalAckMsgSize := int64(0)

		fixedPageSizeKey, err := constructKey(PageMsgSizeTitle, topicName)
		if err != nil {
			return err
		}
		for _, pID := range pageIDs {
			if pID <= minBeginID {
				// Update acked info for message pID
				pageAckedTsKey := path.Join(fixedAckedTsKey, strconv.FormatInt(pID, 10))
				ackedTsKvs[pageAckedTsKey] = nowTs

				// get current page message size
				pageMsgSizeKey := path.Join(fixedPageSizeKey, strconv.FormatInt(pID, 10))
				pageMsgSizeVal, err := rmq.kv.Load(pageMsgSizeKey)
				if err != nil {
					return err
				}
				pageMsgSize, err := strconv.ParseInt(pageMsgSizeVal, 10, 64)
				if err != nil {
					return err
				}
				totalAckMsgSize += pageMsgSize
			}
		}
		err = rmq.kv.MultiSave(ackedTsKvs)
		if err != nil {
			return err
		}

		ackedSizeKey := AckedSizeTitle + topicName
		ackedSizeVal, err := rmq.kv.Load(ackedSizeKey)
		if err != nil {
			return err
		}
		ackedSize, err := strconv.ParseInt(ackedSizeVal, 10, 64)
		if err != nil {
			return err
		}
		ackedSize += totalAckMsgSize
		err = rmq.kv.Save(ackedSizeKey, strconv.FormatInt(ackedSize, 10))
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateReader create a reader for topic and generate reader name
func (rmq *rocksmq) CreateReader(topicName string, startMsgID UniqueID, messageIDInclusive bool, subscriptionRolePrefix string) (string, error) {
	if rmq.isClosed() {
		return "", errors.New(RmqNotServingErrMsg)
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetPrefixSameAsStart(true)
	iter := rmq.store.NewIterator(readOpts)
	fixChanName, err := fixChannelName(topicName)
	if err != nil {
		log.Debug("RocksMQ: fixChannelName " + topicName + " failed")
		return "", err
	}
	dataKey := path.Join(fixChanName, strconv.FormatInt(startMsgID, 10))
	iter.Seek([]byte(dataKey))
	if !iter.Valid() {
		log.Warn("iterator of startMsgID is invalid")
	}
	nowTs, err := getNowTs(rmq.idAllocator)
	if err != nil {
		return "", errors.New("can't get current ts from rocksmq idAllocator")
	}
	readerName := subscriptionRolePrefix + ReaderNamePrefix + strconv.FormatInt(nowTs, 10)

	reader := &rocksmqReader{
		store:              rmq.store,
		topic:              topicName,
		readerName:         readerName,
		readOpts:           readOpts,
		iter:               iter,
		currentID:          startMsgID,
		messageIDInclusive: messageIDInclusive,
		readerMutex:        make(chan struct{}, 1),
	}
	if vals, ok := rmq.readers.Load(topicName); ok {
		readers := vals.([]*rocksmqReader)
		readers = append(readers, reader)
		rmq.readers.Store(topicName, readers)
	} else {
		readers := make([]*rocksmqReader, 1)
		readers[0] = reader
		rmq.readers.Store(topicName, readers)
	}
	return readerName, nil
}

func (rmq *rocksmq) getReader(topicName, readerName string) *rocksmqReader {
	if vals, ok := rmq.readers.Load(topicName); ok {
		for _, v := range vals.([]*rocksmqReader) {
			if v.readerName == readerName {
				return v
			}
		}
	}
	return nil
}

// ReaderSeek seek a reader to the pointed position
func (rmq *rocksmq) ReaderSeek(topicName string, readerName string, msgID UniqueID) {
	if rmq.isClosed() {
		return
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return
	}
	reader.Seek(msgID)
}

// Next get the next message of reader
func (rmq *rocksmq) Next(ctx context.Context, topicName string, readerName string, messageIDInclusive bool) (*ConsumerMessage, error) {
	if rmq.isClosed() {
		return nil, errors.New(RmqNotServingErrMsg)
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		return nil, fmt.Errorf("reader of %s doesn't exist", topicName)
	}
	return reader.Next(ctx, messageIDInclusive)
}

// HasNext judge whether reader has next message
func (rmq *rocksmq) HasNext(topicName string, readerName string, messageIDInclusive bool) bool {
	if rmq.isClosed() {
		return false
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return false
	}
	return reader.HasNext(messageIDInclusive)
}

// CloseReader close a reader
func (rmq *rocksmq) CloseReader(topicName string, readerName string) {
	if rmq.isClosed() {
		return
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return
	}
	reader.Close()
}
