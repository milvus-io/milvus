package mockkv

import (
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

//  use MemoryKV to mock EtcdKV
func NewEtcdKV() *kv.MemoryKV {
	return kv.NewMemoryKV()
}

//  use MemoryKV to mock EtcdKV
func NewMemoryKV() *kv.MemoryKV {
	return kv.NewMemoryKV()
}