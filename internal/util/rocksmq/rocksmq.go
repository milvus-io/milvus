package rocksmq

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type ProducerMessage struct {
	payload []byte
}

type ConsumerMessage struct {
	msgID   UniqueID
	payload []byte
}

type Channel struct {
	beginOffset UniqueID
	endOffset   UniqueID
}

type ConsumerGroupContext struct {
	currentOffset UniqueID
}

type RocksMQ struct {
	kv       kv.Base
	channels map[string]*Channel
	cgCtxs   map[string]ConsumerGroupContext
	mu       sync.Mutex
}

func NewRocksMQ() *RocksMQ {
	mkv := memkv.NewMemoryKV()
	rmq := &RocksMQ{
		kv: mkv,
	}
	return rmq
}

func (rmq *RocksMQ) checkKeyExist(key string) bool {
	_, err := rmq.kv.Load(key)
	return err == nil
}

func (rmq *RocksMQ) CreateChannel(channelName string) error {
	beginKey := channelName + "/begin_id"
	endKey := channelName + "/end_id"

	// Check if channel exist
	if rmq.checkKeyExist(beginKey) || rmq.checkKeyExist(endKey) {
		return errors.New("Channel " + channelName + " already exists.")
	}

	err := rmq.kv.Save(beginKey, "0")
	if err != nil {
		return err
	}

	err = rmq.kv.Save(endKey, "0")
	if err != nil {
		return err
	}

	channel := &Channel{
		beginOffset: 0,
		endOffset:   0,
	}
	rmq.channels[channelName] = channel
	return nil
}

func (rmq *RocksMQ) DestroyChannel(channelName string) error {
	beginKey := channelName + "/begin_id"
	endKey := channelName + "/end_id"

	err := rmq.kv.Remove(beginKey)
	if err != nil {
		return err
	}

	err = rmq.kv.Remove(endKey)
	if err != nil {
		return err
	}

	return nil
}

func (rmq *RocksMQ) CreateConsumerGroup(groupName string, channelName string) error {
	key := groupName + "/" + channelName + "/current_id"
	if rmq.checkKeyExist(key) {
		return errors.New("ConsumerGroup " + groupName + " already exists.")
	}
	err := rmq.kv.Save(key, "0")
	if err != nil {
		return err
	}

	return nil
}

func (rmq *RocksMQ) DestroyConsumerGroup(groupName string, channelName string) error {
	key := groupName + "/" + channelName + "/current_id"

	err := rmq.kv.Remove(key)
	if err != nil {
		return err
	}

	return nil
}

func (rmq *RocksMQ) Produce(channelName string, messages []ProducerMessage) error {
	return nil
}

func (rmq *RocksMQ) Consume(groupName string, channelName string, n int) ([]ConsumerMessage, error) {
	return nil, nil
}

func (rmq *RocksMQ) Seek(groupName string, channelName string, msgID UniqueID) error {
	return nil
}
