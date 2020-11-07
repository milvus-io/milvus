package mockkv

import (
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

//  use MemoryKV to mock EtcdKV
func NewEtcdKV() *kv.MemoryKV {
	return kv.NewMemoryKV()
}

func NewMemoryKV() *kv.MemoryKV {
	return kv.NewMemoryKV()
}
