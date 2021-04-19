package allocator

import (
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type IdAllocator struct {

}

func (allocator *IdAllocator) Initialize() error {
	return nil
}

func (allocator *IdAllocator) Start() error{
	return nil
}
func (allocator *IdAllocator) Close() error{
	return nil
}

func (allocator *IdAllocator) AllocOne() typeutil.Id {
	return 1
}

func (allocator *IdAllocator) Alloc(count uint32) ([]typeutil.Id, error){
	return make([]typeutil.Id, count), nil
}


func NewIdAllocator() *IdAllocator{
	return &IdAllocator{}
}


