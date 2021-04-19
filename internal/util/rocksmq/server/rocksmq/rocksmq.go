package rocksmq

import (
	"fmt"
	"strconv"
	"sync"

	"errors"

	"github.com/zilliztech/milvus-distributed/internal/allocator"

	"github.com/tecbot/gorocksdb"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
)

type UniqueID = typeutil.UniqueID

const (
	DefaultMessageID        = "-1"
	FixedChannelNameLen     = 32
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

type ProducerMessage struct {
	Payload []byte
}

type ConsumerMessage struct {
	MsgID   UniqueID
	Payload []byte
}

type Channel struct {
	beginOffset UniqueID
	endOffset   UniqueID
}

type ConsumerGroupContext struct {
	currentOffset UniqueID
}

type RocksMQ struct {
	store       *gorocksdb.DB
	kv          kv.Base
	channels    map[string]*Channel
	cgCtxs      map[string]ConsumerGroupContext
	idAllocator allocator.GIDAllocator
	channelMu   map[string]*sync.Mutex

	notify map[string][]*Consumer
}

func NewRocksMQ(name string, idAllocator allocator.GIDAllocator) (*RocksMQ, error) {
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

	rmq := &RocksMQ{
		store:       db,
		kv:          mkv,
		idAllocator: idAllocator,
	}
	rmq.channels = make(map[string]*Channel)
	rmq.notify = make(map[string][]*Consumer)
	rmq.channelMu = make(map[string]*sync.Mutex)
	return rmq, nil
}

func NewProducerMessage(data []byte) *ProducerMessage {
	return &ProducerMessage{
		Payload: data,
	}
}

func (rmq *RocksMQ) checkKeyExist(key string) bool {
	val, _ := rmq.kv.Load(key)
	return val != ""
}

func (rmq *RocksMQ) CreateChannel(channelName string) error {
	beginKey := channelName + "/begin_id"
	endKey := channelName + "/end_id"

	// Check if channel exist
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

	channel := &Channel{
		beginOffset: 0,
		endOffset:   0,
	}
	rmq.channels[channelName] = channel
	rmq.channelMu[channelName] = new(sync.Mutex)
	return nil
}

func (rmq *RocksMQ) DestroyChannel(channelName string) error {
	beginKey := channelName + "/begin_id"
	endKey := channelName + "/end_id"

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

	return nil
}

func (rmq *RocksMQ) CreateConsumerGroup(groupName string, channelName string) (*Consumer, error) {
	key := groupName + "/" + channelName + "/current_id"
	if rmq.checkKeyExist(key) {
		log.Debug("RocksMQ: " + key + " existed.")
		for _, consumer := range rmq.notify[channelName] {
			if consumer.GroupName == groupName {
				return consumer, nil
			}
		}

		return nil, nil
	}
	err := rmq.kv.Save(key, DefaultMessageID)
	if err != nil {
		log.Debug("RocksMQ: save " + key + " failed.")
		return nil, err
	}

	//msgNum := make(chan int, 100)
	consumer := Consumer{
		GroupName:   groupName,
		ChannelName: channelName,
		//MsgNum:      msgNum,
	}
	rmq.notify[channelName] = append(rmq.notify[channelName], &consumer)
	return &consumer, nil
}

func (rmq *RocksMQ) DestroyConsumerGroup(groupName string, channelName string) error {
	key := groupName + "/" + channelName + "/current_id"

	err := rmq.kv.Remove(key)
	if err != nil {
		log.Debug("RocksMQ: remove " + key + " failed.")
		return err
	}

	return nil
}

func (rmq *RocksMQ) Produce(channelName string, messages []ProducerMessage) error {
	rmq.channelMu[channelName].Lock()
	defer rmq.channelMu[channelName].Unlock()
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
		key, err := combKey(channelName, idStart+UniqueID(i))
		if err != nil {
			log.Debug("RocksMQ: combKey(" + channelName + "," + strconv.FormatInt(idStart+UniqueID(i), 10) + ")")
			return err
		}

		batch.Put([]byte(key), messages[i].Payload)
	}

	err = rmq.store.Write(gorocksdb.NewDefaultWriteOptions(), batch)
	if err != nil {
		log.Debug("RocksMQ: write batch failed")
		return err
	}

	/* Step II: Update meta data to kv system */
	kvChannelBeginID := channelName + "/begin_id"
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

	kvChannelEndID := channelName + "/end_id"
	kvValues[kvChannelEndID] = strconv.FormatInt(idEnd, 10)

	err = rmq.kv.MultiSave(kvValues)
	if err != nil {
		log.Debug("RocksMQ: multisave failed")
		return err
	}

	for _, consumer := range rmq.notify[channelName] {
		if consumer.MsgMutex != nil {
			consumer.MsgMutex <- struct{}{}
		}
	}
	return nil
}

func (rmq *RocksMQ) Consume(groupName string, channelName string, n int) ([]ConsumerMessage, error) {
	rmq.channelMu[channelName].Lock()
	defer rmq.channelMu[channelName].Unlock()
	metaKey := groupName + "/" + channelName + "/current_id"
	currentID, err := rmq.kv.Load(metaKey)
	if err != nil {
		log.Debug("RocksMQ: load " + metaKey + " failed")
		return nil, err
	}

	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetPrefixSameAsStart(true)
	iter := rmq.store.NewIterator(readOpts)
	defer iter.Close()

	consumerMessage := make([]ConsumerMessage, 0, n)

	fixChanName, err := fixChannelName(channelName)
	if err != nil {
		log.Debug("RocksMQ: fixChannelName " + channelName + " failed")
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
		log.Debug("RocksMQ: consumerMessage is empty")
		return consumerMessage, nil
	}

	newID := consumerMessage[len(consumerMessage)-1].MsgID
	err = rmq.Seek(groupName, channelName, newID)
	if err != nil {
		log.Debug("RocksMQ: Seek(" + groupName + "," + channelName + "," + strconv.FormatInt(newID, 10) + ") failed")
		return nil, err
	}

	return consumerMessage, nil
}

func (rmq *RocksMQ) Seek(groupName string, channelName string, msgID UniqueID) error {
	/* Step I: Check if key exists */
	key := groupName + "/" + channelName + "/current_id"
	if !rmq.checkKeyExist(key) {
		log.Debug("RocksMQ: channel " + key + " not exists")
		return fmt.Errorf("ConsumerGroup %s, channel %s not exists", groupName, channelName)
	}

	storeKey, err := combKey(channelName, msgID)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + channelName + "," + strconv.FormatInt(msgID, 10) + ") failed")
		return err
	}

	_, err = rmq.store.Get(gorocksdb.NewDefaultReadOptions(), []byte(storeKey))
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
