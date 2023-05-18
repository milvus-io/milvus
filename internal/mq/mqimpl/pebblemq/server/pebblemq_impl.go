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
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	pebblekv "github.com/milvus-io/milvus/internal/kv/pebble"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// UniqueID is the type of message ID
type UniqueID = typeutil.UniqueID

// mqState Pebblemq state
type mqState = int64

// Const variable that will be used in pebblemqs
const (
	DefaultMessageID UniqueID = -1

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

	mqNotServingErrMsg = "MQ is not serving"
)

const (
	// mqStateStopped state stands for just created or stopped `Pebblemq` instance
	mqStateStopped mqState = 0
	// mqStateHealthy state stands for healthy `Pebblemq` instance
	mqStateHealthy mqState = 1
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
	return params.PebblemqCfg.RetentionSizeInMB.GetAsInt64() != -1 || params.PebblemqCfg.RetentionTimeInMinutes.GetAsInt64() != -1
}

var topicMu = sync.Map{}

type pebblemq struct {
	store       *pebble.DB
	kv          kv.BaseKV
	idAllocator allocator.Interface
	storeMu     *sync.Mutex
	consumers   sync.Map
	consumersID sync.Map

	retentionInfo *retentionInfo
	readers       sync.Map
	state         mqState
}

// NewPebbleMQ step:
// 1. New pebblemq instance based on pebble with name and pebblekv with kvname
// 2. Init retention info, load retention info to memory
// 3. Start retention goroutine
func NewPebbleMQ(name string, idAllocator allocator.Interface) (*pebblemq, error) {

	// finish pebble KV
	kvName := name + kvSuffix
	kv, err := pebblekv.NewPebbleKV(kvName)
	if err != nil {
		return nil, err
	}

	db, err := pebble.Open(name, kv.Opts)
	if err != nil {
		return nil, err
	}

	var mqIDAllocator allocator.Interface
	// if user didn't specify id allocator, init one with kv
	if idAllocator == nil {
		allocator := allocator.NewGlobalIDAllocator("pmq_id", kv)
		err = allocator.Initialize()
		if err != nil {
			return nil, err
		}
		mqIDAllocator = allocator
	} else {
		mqIDAllocator = idAllocator
	}

	pmq := &pebblemq{
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
	pmq.retentionInfo = ri

	if checkRetention() {
		pmq.retentionInfo.startRetentionInfo()
	}
	atomic.StoreInt64(&pmq.state, mqStateHealthy)
	// TODO add this to monitor metrics
	go func() {
		for {
			time.Sleep(10 * time.Minute)

			log.Info("Pebblemq stats",
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
			pmq.Info()
		}
	}()

	return pmq, nil
}

func (pmq *pebblemq) isClosed() bool {
	return atomic.LoadInt64(&pmq.state) != mqStateHealthy
}

// Close step:
// 1. Stop retention
// 2. Destroy all consumer groups and topics
// 3. Close pebble instance
func (pmq *pebblemq) Close() {
	atomic.StoreInt64(&pmq.state, mqStateStopped)
	pmq.stopRetention()
	pmq.consumers.Range(func(k, v interface{}) bool {
		// TODO what happened if the server crashed? who handled the destroy consumer group? should we just handled it when pebblemq created?
		// or we should not even make consumer info persistent?
		for _, consumer := range v.([]*Consumer) {
			err := pmq.destroyConsumerGroupInternal(consumer.Topic, consumer.GroupName)
			if err != nil {
				log.Warn("Failed to destroy consumer group in pebblemq!", zap.Any("topic", consumer.Topic), zap.Any("groupName", consumer.GroupName), zap.Any("error", err))
			}
		}
		return true
	})
	pmq.storeMu.Lock()
	defer pmq.storeMu.Unlock()
	pmq.kv.Close()
	pmq.store.Close()
	log.Info("Successfully close pebblemq")
}

// print pmq consumer Info
func (pmq *pebblemq) Info() bool {
	rtn := true
	pmq.consumers.Range(func(key, vals interface{}) bool {
		topic, _ := key.(string)
		consumerList, _ := vals.([]*Consumer)

		minConsumerPosition := UniqueID(-1)
		minConsumerGroupName := ""
		for _, consumer := range consumerList {
			consumerPosition, ok := pmq.getCurrentID(consumer.Topic, consumer.GroupName)
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
		pages, _, err := pmq.kv.LoadWithPrefix(pageTsSizeKey)
		if err != nil {
			log.Error("Pebblemq get page num failed", zap.String("topic", topic))
			rtn = false
			return false
		}

		msgSizeKey := MessageSizeTitle + topic
		msgSizeVal, err := pmq.kv.Load(msgSizeKey)
		if err != nil {
			log.Error("Pebblemq get last page size failed", zap.String("topic", topic))
			rtn = false
			return false
		}

		log.Info("Pebblemq Info",
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

func (pmq *pebblemq) stopRetention() {
	if pmq.retentionInfo != nil {
		pmq.retentionInfo.Stop()
	}
}

// CreateTopic writes initialized messages for topic in rocksdb
func (pmq *pebblemq) CreateTopic(topicName string) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
	}
	start := time.Now()

	// Check if topicName contains "/"
	if strings.Contains(topicName, "/") {
		log.Warn("pebblemq failed to create topic for topic name contains \"/\"", zap.String("topic", topicName))
		return retry.Unrecoverable(fmt.Errorf("topic name = %s contains \"/\"", topicName))
	}

	// topicIDKey is the only identifier of a topic
	topicIDKey := TopicIDTitle + topicName
	val, err := pmq.kv.Load(topicIDKey)
	if err != nil {
		return err
	}
	if val != "" {
		log.Warn("pebblemq topic already exists ", zap.String("topic", topicName))
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
	if err = pmq.kv.MultiSave(kvs); err != nil {
		return retry.Unrecoverable(err)
	}

	pmq.retentionInfo.mutex.Lock()
	defer pmq.retentionInfo.mutex.Unlock()
	pmq.retentionInfo.topicRetetionTime.Insert(topicName, time.Now().Unix())
	log.Debug("Pebblemq create topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// DestroyTopic removes messages for topic in pebblemq
func (pmq *pebblemq) DestroyTopic(topicName string) error {
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

	pmq.consumers.Delete(topicName)

	// clean the topic data it self
	fixTopicName := topicName + "/"
	err := pmq.kv.RemoveWithPrefix(fixTopicName)
	if err != nil {
		return err
	}

	// clean page size info
	pageMsgSizeKey := constructKey(PageMsgSizeTitle, topicName)
	err = pmq.kv.RemoveWithPrefix(pageMsgSizeKey)
	if err != nil {
		return err
	}

	// clean page ts info
	pageMsgTsKey := constructKey(PageTsTitle, topicName)
	err = pmq.kv.RemoveWithPrefix(pageMsgTsKey)
	if err != nil {
		return err
	}

	// cleaned acked ts info
	ackedTsKey := constructKey(AckedTsTitle, topicName)
	err = pmq.kv.RemoveWithPrefix(ackedTsKey)
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
	err = pmq.kv.MultiRemove(removedKeys)
	if err != nil {
		return err
	}

	// clean up retention info
	topicMu.Delete(topicName)
	pmq.retentionInfo.topicRetetionTime.GetAndRemove(topicName)

	log.Debug("Pebblemq destroy topic successfully ", zap.String("topic", topicName), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// ExistConsumerGroup check if a consumer exists and return the existed consumer
func (pmq *pebblemq) ExistConsumerGroup(topicName, groupName string) (bool, *Consumer, error) {
	key := constructCurrentID(topicName, groupName)
	_, ok := pmq.consumersID.Load(key)
	if ok {
		if vals, ok := pmq.consumers.Load(topicName); ok {
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
func (pmq *pebblemq) CreateConsumerGroup(topicName, groupName string) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
	}
	start := time.Now()
	key := constructCurrentID(topicName, groupName)
	_, ok := pmq.consumersID.Load(key)
	if ok {
		return fmt.Errorf("pmq CreateConsumerGroup key already exists, key = %s", key)
	}
	pmq.consumersID.Store(key, DefaultMessageID)
	log.Debug("Pebblemq create consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// RegisterConsumer registers a consumer in pebblemq consumers
func (pmq *pebblemq) RegisterConsumer(consumer *Consumer) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
	}
	start := time.Now()
	if vals, ok := pmq.consumers.Load(consumer.Topic); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == consumer.GroupName {
				return nil
			}
		}
		consumers := vals.([]*Consumer)
		consumers = append(consumers, consumer)
		pmq.consumers.Store(consumer.Topic, consumers)
	} else {
		consumers := make([]*Consumer, 1)
		consumers[0] = consumer
		pmq.consumers.Store(consumer.Topic, consumers)
	}
	log.Debug("Pebblemq register consumer successfully ", zap.String("topic", consumer.Topic), zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

func (pmq *pebblemq) GetLatestMsg(topicName string) (int64, error) {
	if pmq.isClosed() {
		return DefaultMessageID, errors.New(mqNotServingErrMsg)
	}
	msgID, err := pmq.getLatestMsg(topicName)
	if err != nil {
		return DefaultMessageID, err
	}

	return msgID, nil
}

// DestroyConsumerGroup removes a consumer group from rocksdb_kv
func (pmq *pebblemq) DestroyConsumerGroup(topicName, groupName string) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
	}
	return pmq.destroyConsumerGroupInternal(topicName, groupName)
}

// DestroyConsumerGroup removes a consumer group from rocksdb_kv
func (pmq *pebblemq) destroyConsumerGroupInternal(topicName, groupName string) error {
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
	pmq.consumersID.Delete(key)
	if vals, ok := pmq.consumers.Load(topicName); ok {
		consumers := vals.([]*Consumer)
		for index, v := range consumers {
			if v.GroupName == groupName {
				close(v.MsgMutex)
				consumers = append(consumers[:index], consumers[index+1:]...)
				pmq.consumers.Store(topicName, consumers)
				break
			}
		}
	}
	log.Debug("Pebblemq destroy consumer group successfully ", zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.Int64("elapsed", time.Since(start).Milliseconds()))
	return nil
}

// Produce produces messages for topic and updates page infos for retention
func (pmq *pebblemq) Produce(topicName string, messages []ProducerMessage) ([]UniqueID, error) {
	if pmq.isClosed() {
		return nil, errors.New(mqNotServingErrMsg)
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
	idStart, idEnd, err := pmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		return []UniqueID{}, err
	}
	allocTime := time.Since(start).Milliseconds()
	if UniqueID(msgLen) != idEnd-idStart {
		return []UniqueID{}, errors.New("Obtained id length is not equal that of message")
	}

	// Insert data to store system
	writeOpts := pebble.WriteOptions{}
	batch := pmq.store.NewBatch()
	msgSizes := make(map[UniqueID]int64)
	msgIDs := make([]UniqueID, msgLen)
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		msgID := idStart + UniqueID(i)
		key := path.Join(topicName, strconv.FormatInt(msgID, 10))
		batch.Set([]byte(key), messages[i].Payload, &writeOpts)
		properties, err := json.Marshal(messages[i].Properties)
		if err != nil {
			log.Warn("properties marshal failed",
				zap.Int64("msgID", msgID),
				zap.String("topicName", topicName),
				zap.Error(err))
			return nil, err
		}
		pKey := path.Join(common.PropertiesKey, topicName, strconv.FormatInt(msgID, 10))
		batch.Set([]byte(pKey), properties, &writeOpts)
		msgIDs[i] = msgID
		msgSizes[msgID] = int64(len(messages[i].Payload))
	}

	err = batch.Commit(&writeOpts)
	if err != nil {
		return []UniqueID{}, err
	}
	writeTime := time.Since(start).Milliseconds()
	if vals, ok := pmq.consumers.Load(topicName); ok {
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
	err = pmq.updatePageInfo(topicName, msgIDs, msgSizes)
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

func (pmq *pebblemq) updatePageInfo(topicName string, msgIDs []UniqueID, msgSizes map[UniqueID]int64) error {
	params := paramtable.Get()
	msgSizeKey := MessageSizeTitle + topicName
	msgSizeVal, err := pmq.kv.Load(msgSizeKey)
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
		if curMsgSize+msgSize > params.PebblemqCfg.PageSize.GetAsInt64() {
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
	err = pmq.kv.MultiSave(mutateBuffer)
	return err
}

func (pmq *pebblemq) getCurrentID(topicName, groupName string) (int64, bool) {
	currentID, ok := pmq.consumersID.Load(constructCurrentID(topicName, groupName))
	if !ok {
		return 0, false
	}
	return currentID.(int64), true
}

func (pmq *pebblemq) getLastID(topicName string) (int64, bool) {
	currentID, ok := pmq.consumersID.Load(topicName)
	if !ok {
		return 0, false
	}
	return currentID.(int64), true
}

// Consume steps:
// 1. Consume n messages from pebble
// 2. Update current_id to the last consumed message
// 3. Update ack informations in pebble
func (pmq *pebblemq) Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error) {
	if pmq.isClosed() {
		return nil, errors.New(mqNotServingErrMsg)
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

	currentID, ok := pmq.getCurrentID(topicName, groupName)
	if !ok {
		return nil, fmt.Errorf("currentID of topicName=%s, groupName=%s not exist", topicName, groupName)
	}
	// return if don't have new message
	lastID, ok := pmq.getLastID(topicName)
	if ok && currentID > lastID {
		return []ConsumerMessage{}, nil
	}
	getLockTime := time.Since(start).Milliseconds()
	prefix := topicName + "/"
	readOpts := pebble.IterOptions{
		UpperBound: []byte(typeutil.AddOne(prefix)),
	}
	iter := pebblekv.NewPebbleIteratorWithUpperBound(pmq.store, &readOpts)
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
		strKey := string(key)
		offset++
		msgID, err := strconv.ParseInt(strKey[len(topicName)+1:], 10, 64)
		if err != nil {
			return nil, err
		}
		askedProperties := path.Join(common.PropertiesKey, topicName, strconv.FormatInt(msgID, 10))
		propertiesValue, closer, err := pmq.store.Get([]byte(askedProperties))
		// pebble will return a ErrNotFound error if the key not exist, let's ignore it here
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		if closer != nil {
			defer closer.Close()
		}
		properties := make(map[string]string)
		if len(propertiesValue) != 0 {
			// before 2.2.0, there have no properties in ProducerMessage and ConsumerMessage in pebblemq
			// when produce before 2.2.0, but consume in 2.2.0, propertiesValue will be []
			if err = json.Unmarshal(propertiesValue, &properties); err != nil {
				return nil, err
			}
		}
		msg := ConsumerMessage{
			MsgID: msgID,
		}
		origData := val
		dataLen := len(origData)
		if dataLen == 0 {
			msg.Payload = nil
			msg.Properties = nil
		} else {
			msg.Payload = make([]byte, dataLen)
			msg.Properties = properties
			copy(msg.Payload, origData)
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

	newID := consumerMessage[len(consumerMessage)-1].MsgID
	moveConsumePosTime := time.Since(start).Milliseconds()

	err := pmq.moveConsumePos(topicName, groupName, newID+1)
	if err != nil {
		return nil, err
	}

	// TODO add this to monitor metrics
	getConsumeTime := time.Since(start).Milliseconds()
	if getConsumeTime > 200 {
		log.Warn("pebblemq consume too slowly", zap.String("topic", topicName),
			zap.Int64("get lock elapse", getLockTime),
			zap.Int64("iterator elapse", iterTime-getLockTime),
			zap.Int64("moveConsumePosTime elapse", moveConsumePosTime-iterTime),
			zap.Int64("total consume elapse", getConsumeTime))
	}
	return consumerMessage, nil
}

// seek is used for internal call without the topicMu
func (pmq *pebblemq) seek(topicName string, groupName string, msgID UniqueID) error {
	pmq.storeMu.Lock()
	defer pmq.storeMu.Unlock()
	key := constructCurrentID(topicName, groupName)
	_, ok := pmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	storeKey := path.Join(topicName, strconv.FormatInt(msgID, 10))
	val, closer, err := pmq.store.Get([]byte(storeKey))
	// pebble will return a ErrNotFound error if the key not exist, let's ignore it for consistency with rocksdb API
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		log.Warn("PebbleMQ: get " + storeKey + " failed")
		return err
	}
	if closer != nil {
		defer closer.Close()
	}
	if val == nil {
		log.Warn("PebbleMQ: trying to seek to no exist position, reset current id",
			zap.String("topic", topicName), zap.String("group", groupName), zap.Int64("msgId", msgID))
		err := pmq.moveConsumePos(topicName, groupName, DefaultMessageID)
		//skip seek if key is not found, this is the behavior as pulsar
		return err
	}
	/* Step II: update current_id */
	err = pmq.moveConsumePos(topicName, groupName, msgID)
	return err
}

func (pmq *pebblemq) moveConsumePos(topicName string, groupName string, msgID UniqueID) error {
	oldPos, ok := pmq.getCurrentID(topicName, groupName)
	if !ok {
		return errors.New("move unknown consumer")
	}
	if msgID < oldPos {
		log.Warn("RocksMQ: trying to move Consume position backward",
			zap.String("topic", topicName), zap.String("group", groupName), zap.Int64("oldPos", oldPos), zap.Int64("newPos", msgID))
		panic("move consume position backward")
	}

	//update ack if position move forward
	err := pmq.updateAckedInfo(topicName, groupName, oldPos, msgID-1)
	if err != nil {
		log.Warn("failed to update acked info ", zap.String("topic", topicName),
			zap.String("groupName", groupName), zap.Error(err))
		return err
	}

	pmq.consumersID.Store(constructCurrentID(topicName, groupName), msgID)
	return nil
}

// Seek updates the current id to the given msgID
func (pmq *pebblemq) Seek(topicName string, groupName string, msgID UniqueID) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
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

	err := pmq.seek(topicName, groupName, msgID)
	if err != nil {
		return err
	}
	log.Debug("successfully seek", zap.String("topic", topicName), zap.String("group", groupName), zap.Uint64("msgId", uint64(msgID)))
	return nil
}

// Only for test
func (pmq *pebblemq) ForceSeek(topicName string, groupName string, msgID UniqueID) error {
	log.Warn("Use method ForceSeek that only for test")
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
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
	pmq.storeMu.Lock()
	defer pmq.storeMu.Unlock()

	key := constructCurrentID(topicName, groupName)
	_, ok = pmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	pmq.consumersID.Store(key, msgID)

	log.Debug("successfully force seek", zap.String("topic", topicName),
		zap.String("group", groupName), zap.Uint64("msgID", uint64(msgID)))
	return nil
}

// SeekToLatest updates current id to the msg id of latest message + 1
func (pmq *pebblemq) SeekToLatest(topicName, groupName string) error {
	if pmq.isClosed() {
		return errors.New(mqNotServingErrMsg)
	}
	pmq.storeMu.Lock()
	defer pmq.storeMu.Unlock()

	key := constructCurrentID(topicName, groupName)
	_, ok := pmq.consumersID.Load(key)
	if !ok {
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	msgID, err := pmq.getLatestMsg(topicName)
	if err != nil {
		return err
	}

	// current msgID should not be included
	err = pmq.moveConsumePos(topicName, groupName, msgID+1)
	if err != nil {
		return err
	}

	log.Debug("successfully seek to latest", zap.String("topic", topicName),
		zap.String("group", groupName), zap.Uint64("latest", uint64(msgID+1)))
	return nil
}

func (pmq *pebblemq) getLatestMsg(topicName string) (int64, error) {
	readOpts := pebble.IterOptions{}
	iter := pebblekv.NewPebbleIterator(pmq.store, &readOpts)
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
func (pmq *pebblemq) Notify(topicName, groupName string) {
	if vals, ok := pmq.consumers.Load(topicName); ok {
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
func (pmq *pebblemq) updateAckedInfo(topicName, groupName string, firstID UniqueID, lastID UniqueID) error {
	// 1. Try to get the page id between first ID and last ID of ids
	pageMsgPrefix := constructKey(PageMsgSizeTitle, topicName) + "/"
	pageMsgFirstKey := pageMsgPrefix + strconv.FormatInt(firstID, 10)

	readOpts := pebble.IterOptions{
		UpperBound: []byte(typeutil.AddOne(pageMsgPrefix)),
	}
	iter := pebblekv.NewPebbleIteratorWithUpperBound(pmq.kv.(*pebblekv.PebbleKV).DB, &readOpts)
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
	if vals, ok := pmq.consumers.Load(topicName); ok {
		consumers, ok := vals.([]*Consumer)
		if !ok || len(consumers) == 0 {
			return nil
		}

		// find min id of all consumer
		var minBeginID UniqueID = lastID
		for _, consumer := range consumers {
			if consumer.GroupName != groupName {
				beginID, ok := pmq.getCurrentID(consumer.Topic, consumer.GroupName)
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
		err := pmq.kv.MultiSave(ackedTsKvs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pmq *pebblemq) CheckTopicValid(topic string) error {
	// Check if key exists
	log := log.With(zap.String("topic", topic))

	_, ok := topicMu.Load(topic)
	if !ok {
		return merr.WrapErrMqTopicNotFound(topic, "failed to get topic")
	}

	latestMsgID, err := pmq.GetLatestMsg(topic)
	if err != nil {
		return err
	}

	if latestMsgID != DefaultMessageID {
		return merr.WrapErrMqTopicNotEmpty(topic, "topic is not empty")
	}
	log.Info("created topic is empty")
	return nil
}
