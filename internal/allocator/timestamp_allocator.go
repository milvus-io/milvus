package allocator

import (
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type TimestampAllocator struct {}

func (allocator *TimestampAllocator) Start() error{
	return nil
}

func (allocator *TimestampAllocator) Close() error{
	return nil
}

func (allocator *TimestampAllocator) AllocOne() (typeutil.Timestamp, error){
	ret, err := allocator.Alloc(1)
	if err != nil{
		return typeutil.ZeroTimestamp, err
	}
	return ret[0], nil
}

func (allocator *TimestampAllocator) Alloc(count uint32) ([]typeutil.Timestamp, error){
	// to do lock and accuire more by grpc request
	return make([]typeutil.Timestamp, count), nil
}

func NewTimestampAllocator() *TimestampAllocator{
	return &TimestampAllocator{}
}
