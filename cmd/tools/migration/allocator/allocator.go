package allocator

import "github.com/milvus-io/milvus/pkg/util/typeutil"

type Allocator interface {
	AllocID() (typeutil.UniqueID, error)
}
