package rocksmq

import (
	"sync"

	rocksdbkv "github.com/zilliztech/milvus-distributed/internal/kv/rocksdb"
)

var Rmq *RocksMQ
var once sync.Once

type Consumer struct {
	GroupName   string
	ChannelName string
	MsgNum      chan int
}

func InitRmq(rocksdbName string, idAllocator IDAllocator) error {
	var err error
	Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
	return err
}

func InitRocksMQ(rocksdbName string) error {
	var err error
	once.Do(func() {
		kvname := rocksdbName + "_kv"
		rocksdbKV, err := rocksdbkv.NewRocksdbKV(kvname)
		if err != nil {
			panic(err)
		}
		idAllocator := NewGlobalIDAllocator("rmq_id", rocksdbKV)
		_ = idAllocator.Initialize()

		Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
		if err != nil {
			panic(err)
		}
	})
	return err
}

func CloseRocksMQ() {
	if Rmq != nil && Rmq.store != nil {
		Rmq.store.Close()
	}
}
