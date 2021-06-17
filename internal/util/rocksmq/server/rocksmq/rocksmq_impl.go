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
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/tecbot/gorocksdb"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
)

type UniqueID = typeutil.UniqueID

const (
	DefaultMessageID        = "-1"
	FixedChannelNameLen     = 320
	RocksDBLRUCacheCapacity = 3 << 30
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

type rocksmq struct {
	store       *gorocksdb.DB
	kv          kv.BaseKV
	idAllocator allocator.GIDAllocator
	channelMu   sync.Map

	consumers sync.Map
}

func NewRocksMQ(name string, idAllocator allocator.GIDAllocator) (*rocksmq, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(RocksDBLRUCacheCapacity))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(FixedChannelNameLen + 1))

	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}

	mkv := memkv.NewMemoryKV()

	rmq := &rocksmq{
		store:       db,
		kv:          mkv,
		idAllocator: idAllocator,
	}
	rmq.channelMu = sync.Map{}
	rmq.consumers = sync.Map{}
	return rmq, nil
}

func (rmq *rocksmq) checkKeyExist(key string) bool {
	val, _ := rmq.kv.Load(key)
	return val != ""
}

func (rmq *rocksmq) CreateTopic(topicName string) error {
	beginKey := topicName + "/begin_id"
	endKey := topicName + "/end_id"

	// Check if topic exist
	if rmq.checkKeyExist(beginKey) || rmq.checkKeyExist(endKey) {
		log.Debug("RocksMQ: " + beginKey + " or " + endKey + " existed.")
		return nil
	}

	err := rmq.kv.Save(beginKey, "0")
	if err != nil {
		log.Debug("RocksMQ: save " + beginKey + " failed.")
		return err
	}

	err = rmq.kv.Save(endKey, "0")
	if err != nil {
		log.Debug("RocksMQ: save " + endKey + " failed.")
		return err
	}
	rmq.channelMu.Store(topicName, new(sync.Mutex))

	return nil
}

func (rmq *rocksmq) DestroyTopic(topicName string) error {
	beginKey := topicName + "/begin_id"
	endKey := topicName + "/end_id"

	err := rmq.kv.Remove(beginKey)
	if err != nil {
		log.Debug("RocksMQ: remove " + beginKey + " failed.")
		return err
	}

	err = rmq.kv.Remove(endKey)
	if err != nil {
		log.Debug("RocksMQ: remove " + endKey + " failed.")
		return err
	}

	rmq.consumers.Delete(topicName)
	log.Debug("DestroyTopic: " + topicName)

	return nil
}

func (rmq *rocksmq) ExistConsumerGroup(topicName, groupName string) (bool, *Consumer) {
	key := groupName + "/" + topicName + "/current_id"
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

func (rmq *rocksmq) CreateConsumerGroup(topicName, groupName string) error {
	key := groupName + "/" + topicName + "/current_id"
	if rmq.checkKeyExist(key) {
		log.Debug("RocksMQ: " + key + " existed.")
		return nil
	}
	err := rmq.kv.Save(key, DefaultMessageID)
	if err != nil {
		log.Debug("RocksMQ: save " + key + " failed.")
		return err
	}

	return nil
}

func (rmq *rocksmq) RegisterConsumer(consumer *Consumer) {
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
}

func (rmq *rocksmq) DestroyConsumerGroup(topicName, groupName string) error {
	key := groupName + "/" + topicName + "/current_id"

	err := rmq.kv.Remove(key)
	if err != nil {
		log.Debug("RocksMQ: remove " + key + " failed.")
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

	log.Debug("DestroyConsumerGroup: " + topicName + "+" + groupName)

	return nil
}

func (rmq *rocksmq) Produce(topicName string, messages []ProducerMessage) error {
	ll, ok := rmq.channelMu.Load(topicName)
	if !ok {
		return fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	msgLen := len(messages)
	idStart, idEnd, err := rmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		log.Debug("RocksMQ: alloc id failed.")
		return err
	}

	if UniqueID(msgLen) != idEnd-idStart {
		log.Debug("RocksMQ: Obtained id length is not equal that of message")
		return errors.New("Obtained id length is not equal that of message")
	}

	/* Step I: Insert data to store system */
	batch := gorocksdb.NewWriteBatch()
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		key, err := combKey(topicName, idStart+UniqueID(i))
		if err != nil {
			log.Debug("RocksMQ: combKey(" + topicName + "," + strconv.FormatInt(idStart+UniqueID(i), 10) + ")")
			return err
		}

		batch.Put([]byte(key), messages[i].Payload)
	}

	err = rmq.store.Write(gorocksdb.NewDefaultWriteOptions(), batch)
	batch.Destroy()
	if err != nil {
		log.Debug("RocksMQ: write batch failed")
		return err
	}

	/* Step II: Update meta data to kv system */
	kvChannelBeginID := topicName + "/begin_id"
	beginIDValue, err := rmq.kv.Load(kvChannelBeginID)
	if err != nil {
		log.Debug("RocksMQ: load " + kvChannelBeginID + " failed")
		return err
	}

	kvValues := make(map[string]string)

	if beginIDValue == "0" {
		log.Debug("RocksMQ: overwrite " + kvChannelBeginID + " with " + strconv.FormatInt(idStart, 10))
		kvValues[kvChannelBeginID] = strconv.FormatInt(idStart, 10)
	}

	kvChannelEndID := topicName + "/end_id"
	kvValues[kvChannelEndID] = strconv.FormatInt(idEnd, 10)

	err = rmq.kv.MultiSave(kvValues)
	if err != nil {
		log.Debug("RocksMQ: multisave failed")
		return err
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
	return nil
}

func (rmq *rocksmq) Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error) {
	ll, ok := rmq.channelMu.Load(topicName)
	if !ok {
		return nil, fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return nil, fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	metaKey := groupName + "/" + topicName + "/current_id"
	currentID, err := rmq.kv.Load(metaKey)
	if err != nil {
		log.Debug("RocksMQ: load " + metaKey + " failed")
		return nil, err
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
	dataKey := fixChanName + "/" + currentID

	// msgID is DefaultMessageID means this is the first consume operation
	// currentID may be not valid if the deprecated values has been removed, when
	// we move currentID to first location.
	// Note that we assume currentId is always correct and not larger than the latest endID.
	if iter.Seek([]byte(dataKey)); currentID != DefaultMessageID && iter.Valid() {
		iter.Next()
	} else {
		newKey := fixChanName + "/"
		iter.Seek([]byte(newKey))
	}

	offset := 0
	for ; iter.Valid() && offset < n; iter.Next() {
		key := iter.Key()
		val := iter.Value()
		offset++
		msgID, err := strconv.ParseInt(string(key.Data())[FixedChannelNameLen+1:], 10, 64)
		if err != nil {
			log.Debug("RocksMQ: parse int " + string(key.Data())[FixedChannelNameLen+1:] + " failed")
			return nil, err
		}
		msg := ConsumerMessage{
			MsgID:   msgID,
			Payload: val.Data(),
		}
		consumerMessage = append(consumerMessage, msg)
		key.Free()
		val.Free()
	}
	if err := iter.Err(); err != nil {
		log.Debug("RocksMQ: get error from iter.Err()")
		return nil, err
	}

	// When already consume to last mes, an empty slice will be returned
	if len(consumerMessage) == 0 {
		//log.Debug("RocksMQ: consumerMessage is empty")
		return consumerMessage, nil
	}

	newID := consumerMessage[len(consumerMessage)-1].MsgID
	err = rmq.Seek(topicName, groupName, newID)
	if err != nil {
		log.Debug("RocksMQ: Seek(" + groupName + "," + topicName + "," + strconv.FormatInt(newID, 10) + ") failed")
		return nil, err
	}

	return consumerMessage, nil
}

func (rmq *rocksmq) Seek(topicName string, groupName string, msgID UniqueID) error {
	/* Step I: Check if key exists */
	key := groupName + "/" + topicName + "/current_id"
	if !rmq.checkKeyExist(key) {
		log.Debug("RocksMQ: channel " + key + " not exists")
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, topicName)
	}

	storeKey, err := combKey(topicName, msgID)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + topicName + "," + strconv.FormatInt(msgID, 10) + ") failed")
		return err
	}

	val, err := rmq.store.Get(gorocksdb.NewDefaultReadOptions(), []byte(storeKey))
	defer val.Free()
	if err != nil {
		log.Debug("RocksMQ: get " + storeKey + " failed")
		return err
	}

	/* Step II: Save current_id in kv */
	err = rmq.kv.Save(key, strconv.FormatInt(msgID, 10))
	if err != nil {
		log.Debug("RocksMQ: save " + key + " failed")
		return err
	}

	return nil
}

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
