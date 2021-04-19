package rocksmq

import (
	"strconv"
	"sync"

	"github.com/tecbot/gorocksdb"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
)

type UniqueID = typeutil.UniqueID

const (
	FixedChannelNameLen = 32
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
	//isServing        int64
	store       *gorocksdb.DB
	kv          kv.Base
	channels    map[string]*Channel
	cgCtxs      map[string]ConsumerGroupContext
	idAllocator master.IDAllocator
	mu          sync.Mutex
	//ctx              context.Context
	//serverLoopWg     sync.WaitGroup
	//serverLoopCtx    context.Context
	//serverLoopCancel func()

	//// tso ticker
	//tsoTicker *time.Ticker
}

func NewRocksMQ() *RocksMQ {
	mkv := memkv.NewMemoryKV()
	// mstore, _ :=
	rmq := &RocksMQ{
		// store: mstore,
		kv: mkv,
	}
	return rmq
}

//func (rmq *RocksMQ) startServerLoop(ctx context.Context) error {
//	rmq.serverLoopCtx, rmq.serverLoopCancel = context.WithCancel(ctx)
//
//	go rmq.tsLoop()
//
//	return nil
//}

//func (rmq *RocksMQ) stopServerLoop() {
//	rmq.serverLoopCancel()
//	rmq.serverLoopWg.Wait()
//}

//func (rmq *RocksMQ) tsLoop() {
//	defer rmq.serverLoopWg.Done()
//	rmq.tsoTicker = time.NewTicker(master.UpdateTimestampStep)
//	defer rmq.tsoTicker.Stop()
//	ctx, cancel := context.WithCancel(rmq.serverLoopCtx)
//	defer cancel()
//
//	for {
//		select {
//		case <-rmq.tsoTicker.C:
//			if err := rmq.idAllocator.UpdateID(); err != nil {
//				log.Println("failed to update id", err)
//				return
//			}
//		case <-ctx.Done():
//			// Server is closed and it should return nil.
//			log.Println("tsLoop is closed")
//			return
//		}
//	}
//}

//func (rmq *RocksMQ) Start() error {
//	//init idAllocator
//	// TODO(yhz): id allocator, which need to etcd address and path, where
//	// we hardcode about the etcd path
//	rmq.idAllocator = master.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{""}, "stand-alone/rocksmq", "gid"))
//	if err := rmq.idAllocator.Initialize(); err != nil {
//		return err
//	}
//
//	// start server loop
//	if err := rmq.startServerLoop(rmq.ctx); err != nil {
//		return err
//	}
//
//	atomic.StoreInt64(&rmq.isServing, 1)
//
//	return nil
//}

//func (rmq *RocksMQ) Stop() error {
//	if !atomic.CompareAndSwapInt64(&rmq.isServing, 1, 0) {
//		// server is already closed
//		return nil
//	}
//
//	log.Print("closing server")
//
//	rmq.stopServerLoop()
//
//	rmq.kv.Close()
//	rmq.store.Close()
//
//	return nil
//}

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
	msgLen := len(messages)
	idStart, idEnd, err := rmq.idAllocator.Alloc(uint32(msgLen))

	if err != nil {
		return err
	}

	// TODO(yhz): Here assume allocated id size is equal to message size
	if UniqueID(msgLen) != idEnd-idStart {
		return errors.New("Obtained id length is not equal that of message")
	}

	/* Step I: Insert data to store system */
	batch := gorocksdb.NewWriteBatch()
	for i := 0; i < msgLen && idStart+UniqueID(i) < idEnd; i++ {
		key, err := combKey(channelName, idStart+UniqueID(i))
		if err != nil {
			return err
		}

		batch.Put([]byte(key), messages[i].payload)
	}

	err = rmq.store.Write(gorocksdb.NewDefaultWriteOptions(), batch)
	if err != nil {
		return err
	}

	/* Step II: Update meta data to kv system */
	kvChannelBeginID := channelName + "/begin_id"
	beginIDValue, err := rmq.kv.Load(kvChannelBeginID)
	if err != nil {
		return err
	}

	kvValues := make(map[string]string)

	if beginIDValue == "0" {
		kvValues[kvChannelBeginID] = strconv.FormatInt(idStart, 10)
	}

	kvChannelEndID := channelName + "/end_id"
	kvValues[kvChannelEndID] = strconv.FormatInt(idEnd, 10)

	return rmq.kv.MultiSave(kvValues)
}

func (rmq *RocksMQ) Consume(groupName string, channelName string, n int) ([]ConsumerMessage, error) {
	return nil, nil
}

func (rmq *RocksMQ) Seek(groupName string, channelName string, msgID UniqueID) error {
	/* Step I: Check if key exists */
	key := groupName + "/" + channelName + "/current_id"
	if !rmq.checkKeyExist(key) {
		return errors.New("ConsumerGroup " + groupName + ", channel " + channelName + " not exists.")
	}

	storeKey, err := combKey(channelName, msgID)
	if err != nil {
		return err
	}

	_, err = rmq.store.Get(gorocksdb.NewDefaultReadOptions(), []byte(storeKey))
	if err != nil {
		return err
	}

	/* Step II: Save current_id in kv */
	err = rmq.kv.Save(key, strconv.FormatInt(msgID, 10))
	if err != nil {
		return err
	}

	return nil
}
