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
	"errors"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	pebblekv "github.com/milvus-io/milvus/internal/kv/pebble"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"
)

// UniqueID is the type of message ID
type UniqueID = typeutil.UniqueID

// RmqState PebbleMQ state
type RmqState = int64

// PebbleMQPageSize is the size of a message page, default 256MB
var PebbleMQPageSize int64 = 256 << 20

// Const variable that will be used in pebblemqs
const (
	DefaultMessageID = -1

	kvSuffix = "_meta_kv"

	//  topic_begin_id/topicName
	// topic begin id record a topic is valid, create when topic is created, cleaned up on destroy topic
	TopicIDTitle = "topic_id/"

	// message_size/topicName record the current page message size, once current message size > PebbleMQ size, reset this value and open a new page
	// TODO should be cached
	MessageSizeTitle = "message_size/"

	// page_message_size/topicName/pageId record the endId of each page, it will be purged either in retention or the destroy of topic
	PageMsgSizeTitle = "page_message_size/"

	// page_ts/topicName/pageId, record the page last ts, used for TTL functionality
	PageTsTitle = "page_ts/"

	// acked_ts/topicName/pageId, record the latest ack ts of each page, will be purged on retention or destroy of the topic
	AckedTsTitle = "acked_ts/"

	// only in memory
	CurrentIDSuffix = "current_id"

	ReaderNamePrefix = "reader-"

	RmqNotServingErrMsg = "PebbleMQ is not serving"
)

const (
	// RmqStateStopped state stands for just created or stopped `PebbleMQ` instance
	RmqStateStopped RmqState = 0
	// RmqStateHealthy state stands for healthy `PebbleMQ` instance
	RmqStateHealthy RmqState = 1
)

/**
 * Construct current id
 */
func constructCurrentID(topicName, groupName string) string {
	return groupName + "/" + topicName + "/" + CurrentIDSuffix
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
	return PebbleMQRetentionTimeInSecs != -1 || PebbleMQRetentionSizeInMB != -1
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

var topicMu = sync.Map{}

type pebblemq struct {
	store       *pebble.DB
	kv          kv.BaseKV
	idAllocator allocator.GIDAllocator
	storeMu     *sync.Mutex
	consumers   sync.Map
	consumersID sync.Map

	retentionInfo *retentionInfo
	readers       sync.Map
	state         RmqState
}

// NewPebbleMQ step:
// 1. New pebblemq instance based on pebble with name and pebblekv with kvname
// 2. Init retention info, load retention info to memory
// 3. Start retention goroutine
func NewPebbleMQ(name string, idAllocator allocator.GIDAllocator) (*pebblemq, error) {
	kvName := name + kvSuffix
	kv, err := pebblekv.NewPebbleKV(kvName)
	if err != nil {
		return nil, err
	}

	db, err := pebble.Open(name, kv.Opts)
	if err != nil {
		return nil, err
	}

	var mqIDAllocator allocator.GIDAllocator
	// if user didn't specify id allocator, init one with kv
	if idAllocator == nil {
		allocator := allocator.NewGlobalIDAllocator("rmq_id", kv)
		err = allocator.Initialize()
		if err != nil {
			return nil, err
		}
		mqIDAllocator = allocator
	} else {
		mqIDAllocator = idAllocator
	}

	rmq := &pebblemq{
		store:       db,
		kv:          kv,
		idAllocator: mqIDAllocator,
		storeMu:     &sync.Mutex{},
		consumers:   sync.Map{},
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
	// TODO add this to monitor metrics
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			log.Info("PebbleMQ stats",
				zap.String("cache", strconv.FormatInt(kv.DB.Metrics().BlockCache.Count, 10)),
				zap.String("pebblekv memtable ", strconv.FormatInt(kv.DB.Metrics().MemTable.Count, 10)),
				zap.String("pebblekv table readers", strconv.FormatInt(kv.DB.Metrics().TableIters, 10)),
				zap.String("pebblekv pinned", strconv.FormatInt(kv.DB.Metrics().TableCache.Count, 10)),
				zap.String("store memtable ", strconv.FormatInt(db.Metrics().MemTable.Count, 10)),
				zap.String("store table readers", strconv.FormatInt(db.Metrics().TableIters, 10)),
				zap.String("store pinned", strconv.FormatInt(db.Metrics().TableCache.Count, 10)),
				zap.String("store l0 file num", strconv.FormatInt(db.Metrics().Levels[0].NumFiles, 10)),
				zap.String("store l1 file num", strconv.FormatInt(db.Metrics().Levels[1].NumFiles, 10)),
				zap.String("store l2 file num", strconv.FormatInt(db.Metrics().Levels[2].NumFiles, 10)),
				zap.String("store l3 file num", strconv.FormatInt(db.Metrics().Levels[3].NumFiles, 10)),
				zap.String("store l4 file num", strconv.FormatInt(db.Metrics().Levels[4].NumFiles, 10)),
			)
		}
	}()

	return rmq, nil
}

func (rmq *pebblemq) isClosed() bool {
	return atomic.LoadInt64(&rmq.state) != RmqStateHealthy
}

// Close step:
// 1. Stop retention
// 2. Destroy all consumer groups and topics
// 3. Close pebble instance
func (rmq *pebblemq) Close() {
	atomic.StoreInt64(&rmq.state, RmqStateStopped)
	rmq.stopRetention()
	rmq.consumers.Range(func(k, v interface{}) bool {
		// TODO what happened if the server crashed? who handled the destroy consumer group? should we just handled it when pebblemq created?
		// or we should not even make consumer info persistent?
		for _, consumer := range v.([]*Consumer) {
			err := rmq.destroyConsumerGroupInternal(consumer.Topic, consumer.GroupName)
			if err != nil {
				log.Warn("Failed to destroy consumer group in pebblemq!", zap.Any("topic", consumer.Topic), zap.Any("groupName", consumer.GroupName), zap.Any("error", err))
			}
		}
		return true
	})
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	rmq.kv.Close()
	rmq.store.Close()
	log.Info("Successfully close pebblemq")
}

func (rmq *pebblemq) stopRetention() {
	if rmq.retentionInfo != nil {
		rmq.retentionInfo.Stop()
	}
}

// CreateTopic writes initialized messages for topic in pebble
func (rmq *pebblemq) CreateTopic(topicName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	start := time.Now()

	// Check if topicName contains "/"
	if strings.Contains(topicName, "/") {
		log.Error("pebblemq failed to create topic for topic name contains \"/\"", zap.String("topic", topicName))
		return retry.Unrecoverable(fmt.Errorf("topic name = %s contains \"/\"", topicName))
	}

	// topicIDKey is the only identifier of a topic
	topicIDKey := TopicIDTitle + topicName
	val, err := rmq.kv.Load(topicIDKey)
	if err != nil {
		return err
	}
	if val != "" {
		log.Debug("pebblemq topic already exists ", zap.String("topic", topicName))
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
	if err = rmq.kv.MultiSave(kvs); err != nil {
		return retry.Unrecoverable(err)
	}

	rmq.retentionInfo.mutex.Lock()
	defer rmq.retentionInfo.mutex.Unlock()
	rmq.retentionInfo.topicRetetionTime.Store(topicName, time.Now().Unix())
	log.Debug("PebbleMQ create topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// DestroyTopic removes messages for topic in pebblemq
func (rmq *pebblemq) DestroyTopic(topicName string) error {
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

	// clean the topic data it self
	fixTopicName := topicName + "/"
	err := rmq.kv.RemoveWithPrefix(fixTopicName)
	if err != nil {
		return err
	}

	// clean page size info
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(pageMsgSizeKey)
	if err != nil {
		return err
	}

	// clean page ts info
	pageMsgTsKey := constructKey(PageTsTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(pageMsgTsKey)
	if err != nil {
		return err
	}

	// cleaned acked ts info
	ackedTsKey := constructKey(AckedTsTitle, topicName)
	err = rmq.kv.RemoveWithPrefix(ackedTsKey)
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
	err = rmq.kv.MultiRemove(removedKeys)
	if err != nil {
		return err
	}

	// clean up retention info
	topicMu.Delete(topicName)
	rmq.retentionInfo.topicRetetionTime.Delete(topicName)

	// clean up reader
	if val, ok := rmq.readers.LoadAndDelete(topicName); ok {
		for _, reader := range val.([]*pebbleReader) {
			reader.Close()
		}
	}
	log.Debug("PebbleMQ destroy topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// ExistConsumerGroup check if a consumer exists and return the existed consumer
func (rmq *pebblemq) ExistConsumerGroup(topicName, groupName string) (bool, *Consumer, error) {
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
func (rmq *pebblemq) CreateConsumerGroup(topicName, groupName string) error {
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
	log.Debug("PebbleMQ create consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// RegisterConsumer registers a consumer in pebblemq consumers
func (rmq *pebblemq) RegisterConsumer(consumer *Consumer) error {
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
	log.Debug("PebbleMQ register consumer successfully ", zap.String("topic", consumer.Topic), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (rmq *pebblemq) GetLatestMsg(topicName string) (int64, error) {
	if rmq.isClosed() {
		return DefaultMessageID, errors.New(RmqNotServingErrMsg)
	}
	msgID, err := rmq.getLatestMsg(topicName)
	if err != nil {
		return DefaultMessageID, err
	}

	return msgID, nil
}

// DestroyConsumerGroup removes a consumer group from pebble_kv
func (rmq *pebblemq) DestroyConsumerGroup(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	return rmq.destroyConsumerGroupInternal(topicName, groupName)
}

// DestroyConsumerGroup removes a consumer group from pebble_kv
func (rmq *pebblemq) destroyConsumerGroupInternal(topicName, groupName string) error {
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
	log.Debug("PebbleMQ destroy consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// Produce produces messages for topic and updates page infos for retention
func (rmq *pebblemq) Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error) {
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
		log.Error("PebbleMQ: alloc id failed.", zap.Error(err))
		return []UniqueID{}, err
	}
	allocTime := time.Since(start).Milliseconds()
	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}

	// Insert data to store system
	writeOptions := pebble.WriteOptions{}
	batch := rmq.store.NewBatch()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key := path.Join(topicName, strconv.FormatInt(msgID, 10))
		batch.Set([]byte(key), messages[i].Payload, &writeOptions)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	err = batch.Commit(&writeOptions)
	if err != nil {
		log.Debug("PebbleMQ: write batch failed")
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

	// Notify reader
	if val, ok := rmq.readers.Load(topicName); ok {
		for _, reader := range val.([]*pebbleReader) {
			select {
			case reader.readerMutex <- struct{}{}:
			default:
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
		log.Warn("pebblemq produce too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("alloc elapse", allocTime-getLockTime),
			zap.Int64("write elapse", writeTime-allocTime),
			zap.Int64("updatePage elapse", getProduceTime-writeTime),
			zap.Int64("produce total elapse", getProduceTime),
		)
	}
	return msgIDs, nil
}

func (rmq *pebblemq) updatePageInfo(topicName string, msgIDs []UniqueID, msgSizes map[UniqueID]int64) error {
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := rmq.kv.Load(msgSizeKey)
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
		if curMsgSize+msgSize > PebbleMQPageSize {
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
	err = rmq.kv.MultiSave(mutateBuffer)
	return err
}

// Consume steps:
// 1. Consume n messages from pebble
// 2. Update current_id to the last consumed message
// 3. Update ack informations in pebble
func (rmq *pebblemq) Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error) {
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
	currentID, ok := rmq.consumersID.Load(metaKey)
	if !ok {
		return nil, fmt.Errorf("currentID of topicName=%s, groupName=%s not exist", topicName, groupName)
	}

	readOpts := pebble.IterOptions{}
	prefix := topicName + "/"
	readOpts.UpperBound = []byte(typeutil.AddOne(prefix))
	iter := pebblekv.NewPebbleIteratorWithUpperBound(rmq.store, &readOpts)
	defer iter.Close()

	var dataKey string
	if currentID == DefaultMessageID {
		dataKey = prefix
	} else {
		dataKey = path.Join(topicName, strconv.FormatInt(currentID.(int64), 10))
	}
	iter.Seek([]byte(dataKey))
	consumerMessage := make([]ConsumerMessage, 0, n)
	offset := 0
	for ; iter.Valid() && offset < n; iter.Next() {
		key := iter.Key()
		val := iter.Value()
		strKey := string(key)
		offset++
		msgID, err := strconv.ParseInt(strKey[len(topicName)+1:], 10, 64)
		if err != nil {
			log.Warn("PebbleMQ: parse int " + strKey[len(topicName)+1:] + " failed")
			return nil, err
		}
		msg := ConsumerMessage{
			MsgID: msgID,
		}
		dataLen := len(val)
		if dataLen == 0 {
			msg.Payload = nil
		} else {
			msg.Payload = make([]byte, dataLen)
			copy(msg.Payload, val)
		}
		consumerMessage = append(consumerMessage, msg)
	}
	// if iterate fail
	if err := iter.Err(); err != nil {
		return nil, err
	}
	iterTime := time.Since(start).Milliseconds()

	// When already consume to last mes, an empty slice will be returned
	if len(consumerMessage) == 0 {
		// log.Debug("PebbleMQ: consumerMessage is empty")
		return consumerMessage, nil
	}

	consumedIDs := make([]UniqueID, 0, len(consumerMessage))
	for _, msg := range consumerMessage {
		consumedIDs = append(consumedIDs, msg.MsgID)
	}
	newID := consumedIDs[len(consumedIDs)-1]
	err := rmq.updateAckedInfo(topicName, groupName, consumedIDs)
	if err != nil {
		log.Warn("failed to update acked info ", zap.String("topic", topicName),
			zap.String("groupName", groupName), zap.Error(err))
		return nil, err
	}
	updateAckedTime := time.Since(start).Milliseconds()
	rmq.moveConsumePos(topicName, groupName, newID+1)

	// TODO add this to monitor metrics
	getConsumeTime := time.Since(start).Milliseconds()
	if getConsumeTime > 200 {
		log.Warn("pebblemq consume too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("iterator elapse", iterTime-getLockTime),
			zap.Int64("updateAckedInfo elapse", updateAckedTime-iterTime),
			zap.Int64("total consume elapse", getConsumeTime))
	}
	return consumerMessage, nil
}

// seek is used for internal call without the topicMu
func (rmq *pebblemq) seek(topicName string, groupName string, msgID UniqueID) error {
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if !ok {
		log.Warn("PebbleMQ: channel " + key + " not exists")
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	storeKey := path.Join(topicName, strconv.FormatInt(msgID, 10))
	val, closer, err := rmq.store.Get([]byte(storeKey))
	if err != nil && err != pebble.ErrNotFound {
		log.Warn("PebbleMQ: get " + storeKey + " failed")
		return err
	}
	if closer != nil {
		defer closer.Close()
	}
	if val == nil {
		log.Warn("PebbleMQ: trying to seek to no exist position, reset current id",
			zap.String("topic", topicName), zap.String("group", groupName), zap.Int64("msgId", msgID))
		rmq.moveConsumePos(topicName, groupName, DefaultMessageID)
		//skip seek if key is not found, this is the behavior as pulsar
		return nil
	}
	/* Step II: update current_id */
	rmq.moveConsumePos(topicName, groupName, msgID)
	return nil
}

func (rmq *pebblemq) moveConsumePos(topicName string, groupName string, msgID UniqueID) {
	key := constructCurrentID(topicName, groupName)
	rmq.consumersID.Store(key, msgID)
}

// Seek updates the current id to the given msgID
func (rmq *pebblemq) Seek(topicName string, groupName string, msgID UniqueID) error {
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

	err := rmq.seek(topicName, groupName, msgID)
	if err != nil {
		log.Debug("failed to seek", zap.String("topic", topicName), zap.String("group", groupName), zap.Uint64("msgId", uint64(msgID)), zap.Error(err))
		return err
	}
	log.Debug("successfully seek", zap.String("topic", topicName), zap.String("group", groupName), zap.Uint64("msgId", uint64(msgID)))
	return nil
}

// SeekToLatest updates current id to the msg id of latest message + 1
func (rmq *pebblemq) SeekToLatest(topicName, groupName string) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	rmq.storeMu.Lock()
	defer rmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	_, ok := rmq.consumersID.Load(key)
	if !ok {
		log.Warn("PebbleMQ: channel " + key + " not exists")
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	msgID, err := rmq.getLatestMsg(topicName)
	if err != nil {
		return err
	}

	// current msgID should not be included
	rmq.moveConsumePos(topicName, groupName, msgID+1)
	log.Debug("successfully seek to latest", zap.String("topic", topicName),
		zap.String("group", groupName), zap.Uint64("latest", uint64(msgID+1)))
	return nil
}

func (rmq *pebblemq) getLatestMsg(topicName string) (int64, error) {
	readOpts := pebble.IterOptions{}
	iter := pebblekv.NewPebbleIterator(rmq.store, &readOpts)
	defer iter.Close()

	prefix := topicName + "/"
	// seek to the last message of thie topic
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
	seekMsgID := string(iKey)

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
func (rmq *pebblemq) Notify(topicName, groupName string) {
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
func (rmq *pebblemq) updateAckedInfo(topicName, groupName string, ids []UniqueID) error {
	if len(ids) == 0 {
		return nil
	}
	firstID := ids[0]
	lastID := ids[len(ids)-1]

	// 1. Try to get the page id between first ID and last ID of ids
	pageMsgPrefix := constructKey(PageMsgSizeTitle, topicName) + "/"
	pageMsgFirstKey := pageMsgPrefix + strconv.FormatInt(firstID, 10)
	readOpts := pebble.IterOptions{UpperBound: []byte(typeutil.AddOne(pageMsgFirstKey))}

	iter := pebblekv.NewPebbleIteratorWithUpperBound(rmq.kv.(*pebblekv.PebbleKV).DB, &readOpts)
	defer iter.Close()
	var pageIDs []UniqueID

	for iter.Seek([]byte(pageMsgFirstKey)); iter.Valid(); iter.Next() {
		key := iter.Key()
		pageID, err := parsePageID(string(key))
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
			return nil
		}
		// update consumer id
		for _, consumer := range consumers {
			if consumer.GroupName == groupName {
				consumer.beginID = lastID
				break
			}
		}

		// find min id of all consumer
		var minBeginID int64 = math.MaxInt64
		for _, consumer := range consumers {
			if consumer.beginID < minBeginID {
				minBeginID = consumer.beginID
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
		err := rmq.kv.MultiSave(ackedTsKvs)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateReader create a reader for topic and generate reader name
func (rmq *pebblemq) CreateReader(topicName string, startMsgID UniqueID, messageIDInclusive bool, subscriptionRolePrefix string) (string, error) {
	if rmq.isClosed() {
		return "", errors.New(RmqNotServingErrMsg)
	}
	if _, ok := topicMu.Load(topicName); !ok {
		return "", fmt.Errorf("topic=%s not exist", topicName)
	}
	readOpts := pebble.IterOptions{UpperBound: []byte(typeutil.AddOne(topicName + "/"))}
	iter := pebblekv.NewPebbleIteratorWithUpperBound(rmq.store, &readOpts)
	dataKey := path.Join(topicName, strconv.FormatInt(startMsgID, 10))
	iter.Seek([]byte(dataKey))
	// if iterate fail
	if err := iter.Err(); err != nil {
		return "", err
	}

	nowTs, err := getNowTs(rmq.idAllocator)
	if err != nil {
		return "", errors.New("Can't get current ts from pebblemq idAllocator")
	}
	readerName := subscriptionRolePrefix + ReaderNamePrefix + strconv.FormatInt(nowTs, 10)

	reader := &pebbleReader{
		store:              rmq.store,
		topic:              topicName,
		readerName:         readerName,
		readOpts:           &readOpts,
		iter:               iter,
		currentID:          startMsgID,
		messageIDInclusive: messageIDInclusive,
		readerMutex:        make(chan struct{}, 1),
	}
	if vals, ok := rmq.readers.Load(topicName); ok {
		readers := vals.([]*pebbleReader)
		readers = append(readers, reader)
		rmq.readers.Store(topicName, readers)
	} else {
		readers := make([]*pebbleReader, 1)
		readers[0] = reader
		rmq.readers.Store(topicName, readers)
	}
	return readerName, nil
}

func (rmq *pebblemq) getReader(topicName, readerName string) *pebbleReader {
	if vals, ok := rmq.readers.Load(topicName); ok {
		for _, v := range vals.([]*pebbleReader) {
			if v.readerName == readerName {
				return v
			}
		}
	}
	return nil
}

func (rmq *pebblemq) getAndDeleteReader(topicName, readerName string) *pebbleReader {
	if vals, ok := rmq.readers.Load(topicName); ok {
		readers := vals.([]*pebbleReader)
		for i, v := range vals.([]*pebbleReader) {
			if v.readerName == readerName {
				readers[i] = readers[len(readers)-1]
				rmq.readers.Store(topicName, readers[:len(readers)-1])
				return v
			}
		}
	}
	return nil
}

// ReaderSeek seek a reader to the pointed position
func (rmq *pebblemq) ReaderSeek(topicName string, readerName string, msgID UniqueID) error {
	if rmq.isClosed() {
		return errors.New(RmqNotServingErrMsg)
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return fmt.Errorf("reader not exist, topic %s, reader %s", topicName, readerName)
	}
	reader.Seek(msgID)
	return nil
}

// Next get the next message of reader
func (rmq *pebblemq) Next(ctx context.Context, topicName string, readerName string) (*ConsumerMessage, error) {
	if rmq.isClosed() {
		return nil, errors.New(RmqNotServingErrMsg)
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		return nil, fmt.Errorf("reader of %s doesn't exist", topicName)
	}
	return reader.Next(ctx)
}

// HasNext judge whether reader has next message
func (rmq *pebblemq) HasNext(topicName string, readerName string) bool {
	if rmq.isClosed() {
		return false
	}
	reader := rmq.getReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return false
	}
	return reader.HasNext()
}

// CloseReader close a reader
func (rmq *pebblemq) CloseReader(topicName string, readerName string) {
	if rmq.isClosed() {
		return
	}
	reader := rmq.getAndDeleteReader(topicName, readerName)
	if reader == nil {
		log.Warn("reader not exist", zap.String("topic", topicName), zap.String("readerName", readerName))
		return
	}
	reader.Close()
	reader = nil
}
