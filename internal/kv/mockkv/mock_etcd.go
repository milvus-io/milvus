package mockkv

import (
	"github.com/zilliztech/milvus-distributed/internal/util/kvutil"
)

//  use MemoryKV to mock EtcdKV
func NewEtcdKV() *kvutil.MemoryKV {
	return kvutil.NewMemoryKV()
}

func NewMemoryKV() *kvutil.MemoryKV {
	return kvutil.NewMemoryKV()
}
