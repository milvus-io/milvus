package mockkv

import (
	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
)

//  use MemoryKV to mock EtcdKV
func NewEtcdKV() *memkv.MemoryKV {
	return memkv.NewMemoryKV()
}

func NewMemoryKV() *memkv.MemoryKV {
	return memkv.NewMemoryKV()
}
