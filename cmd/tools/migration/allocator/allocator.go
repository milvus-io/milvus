package allocator

import "github.com/milvus-io/milvus/internal/util/typeutil"

type Allocator interface {
	AllocID() (typeutil.UniqueID, error)
}
